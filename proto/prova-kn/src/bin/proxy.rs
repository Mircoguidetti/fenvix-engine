//! Il finto S3 rallentato: un proxy HTTP davanti al finto S3 (moto sulla porta 9000).
//!
//! - Tutto quello che non è GET passa a moto senza ritardo (PUT, creazione del bucket, HEAD…).
//! - GET: la prima volta che arriva una certa coppia (oggetto, Range) la risposta viene da moto e il proxy se la tiene in
//!   RAM; dalla seconda in poi la serve lui. Su ogni GET (anche la prima) il proxy aspetta il tempo pescato dal modello
//!   (`modello.rs`) prima di rispondere. Così il tempo della GET è quello del modello, non quello di moto (che è un
//!   programma Python e su 16 MB in parallelo sarebbe lui il collo di bottiglia).
//! - Se il cliente chiude la connessione prima della risposta (richiesta annullata), il proxy lo conta e non manda i byte.
//!
//! `GET /__stat` restituisce i contatori in JSON; `POST /__modello/<nome>` cambia il modello al volo.

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use prova_kn::modello::Modello;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
struct Arg {
    #[arg(long, default_value = "127.0.0.1:9100")]
    ascolta: String,
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    moto: String,
    #[arg(long, default_value = "base")]
    modello: String,
}

#[derive(Clone)]
struct Risposta {
    stato: u16,
    intest: Vec<(String, Vec<u8>)>,
    corpo: Bytes,
}

struct Stato {
    moto: String,
    modello: Mutex<Modello>,
    cache: Mutex<HashMap<(String, String), Risposta>>,
    client: Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    get: AtomicU64,
    get_annullate: AtomicU64,
    get_da_moto: AtomicU64,
    byte_inviati: AtomicU64,
    altre: AtomicU64,
}

struct Guardia<'a>(&'a Stato, bool);
impl Drop for Guardia<'_> {
    fn drop(&mut self) {
        if !self.1 {
            self.0.get_annullate.fetch_add(1, Relaxed);
        }
    }
}

async fn inoltra(st: &Stato, req: Request<Incoming>) -> anyhow::Result<Risposta> {
    let (parti, corpo) = req.into_parts();
    let corpo = corpo.collect().await?.to_bytes();
    let uri = format!("{}{}", st.moto, parti.uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));
    let mut b = Request::builder().method(parti.method.clone()).uri(uri);
    for (k, v) in parti.headers.iter() {
        b = b.header(k, v);
    }
    let r = st.client.request(b.body(Full::new(corpo))?).await?;
    let stato = r.status().as_u16();
    let intest: Vec<(String, Vec<u8>)> = r.headers().iter().filter(|(k, _)| k.as_str() != "transfer-encoding" && k.as_str() != "connection").map(|(k, v)| (k.to_string(), v.as_bytes().to_vec())).collect();
    let corpo = r.into_body().collect().await?.to_bytes();
    Ok(Risposta { stato, intest, corpo })
}

fn costruisci(r: &Risposta) -> Response<Full<Bytes>> {
    let mut b = Response::builder().status(r.stato);
    for (k, v) in &r.intest {
        if k == "content-length" {
            continue;
        }
        b = b.header(k.as_str(), v.as_slice());
    }
    b.header("content-length", r.corpo.len()).body(Full::new(r.corpo.clone())).unwrap()
}

async fn servi(st: Arc<Stato>, req: Request<Incoming>) -> anyhow::Result<Response<Full<Bytes>>> {
    let path = req.uri().path().to_string();
    if path == "/__stat" {
        let j = serde_json::json!({
            "get": st.get.load(Relaxed), "get_annullate": st.get_annullate.load(Relaxed),
            "get_da_moto": st.get_da_moto.load(Relaxed), "byte_inviati": st.byte_inviati.load(Relaxed),
            "altre": st.altre.load(Relaxed), "modello": *st.modello.lock(),
        });
        return Ok(Response::new(Full::new(Bytes::from(j.to_string()))));
    }
    if let Some(n) = path.strip_prefix("/__modello/") {
        let m = Modello::per_nome(n).ok_or_else(|| anyhow::anyhow!("modello sconosciuto {n}"))?;
        *st.modello.lock() = m;
        return Ok(Response::new(Full::new(Bytes::from(format!("ok {n}")))));
    }
    if req.method() != hyper::Method::GET {
        st.altre.fetch_add(1, Relaxed);
        return Ok(costruisci(&inoltra(&st, req).await?));
    }
    st.get.fetch_add(1, Relaxed);
    let mut g = Guardia(&st, false);
    let range = req.headers().get("range").map(|v| String::from_utf8_lossy(v.as_bytes()).to_string()).unwrap_or_default();
    let chiave = (path, range);
    let in_cache = st.cache.lock().get(&chiave).cloned();
    let r = match in_cache {
        Some(r) => r,
        None => {
            st.get_da_moto.fetch_add(1, Relaxed);
            // in un task a parte: se il cliente annulla, la risposta di moto entra comunque in memoria (altrimenti
            // le fette più lente, quelle annullate, non entrerebbero mai e passerebbero sempre da moto)
            let st2 = st.clone();
            let r = tokio::spawn(async move {
                let r = inoltra(&st2, req).await?;
                if r.stato == 200 || r.stato == 206 {
                    st2.cache.lock().insert(chiave, r.clone());
                }
                Ok::<_, anyhow::Error>(r)
            })
            .await??;
            r
        }
    };
    let ms = {
        let m = *st.modello.lock();
        m.campiona(r.corpo.len() as u64, &mut SmallRng::from_entropy())
    };
    if ms > 0.0 {
        tokio::time::sleep(Duration::from_micros((ms * 1000.0) as u64)).await;
    }
    g.1 = true;
    st.byte_inviati.fetch_add(r.corpo.len() as u64, Relaxed);
    Ok(costruisci(&r))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Arg::parse();
    let modello = Modello::per_nome(&a.modello).ok_or_else(|| anyhow::anyhow!("modello sconosciuto"))?;
    let st = Arc::new(Stato {
        moto: a.moto.clone(),
        modello: Mutex::new(modello),
        cache: Mutex::new(HashMap::new()),
        client: Client::builder(TokioExecutor::new()).pool_max_idle_per_host(64).build_http(),
        get: AtomicU64::new(0),
        get_annullate: AtomicU64::new(0),
        get_da_moto: AtomicU64::new(0),
        byte_inviati: AtomicU64::new(0),
        altre: AtomicU64::new(0),
    });
    let l = tokio::net::TcpListener::bind(&a.ascolta).await?;
    eprintln!("proxy su {} -> {}, modello {:?}", a.ascolta, a.moto, modello);
    loop {
        let (s, _) = l.accept().await?;
        let _ = s.set_nodelay(true);
        let st = st.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let st = st.clone();
                async move {
                    match servi(st, req).await {
                        Ok(r) => Ok::<_, std::convert::Infallible>(r),
                        Err(e) => Ok(Response::builder().status(502).body(Full::new(Bytes::from(e.to_string()))).unwrap()),
                    }
                }
            });
            let _ = http1::Builder::new().serve_connection(TokioIo::new(s), svc).await;
        });
    }
}
