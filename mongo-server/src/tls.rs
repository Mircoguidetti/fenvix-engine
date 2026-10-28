//! TLS for the client listener: mongod's `net.tls.mode` (disabled, allowTLS, preferTLS, requireTLS), a server certificate with
//! its key, an optional CA that client certificates must chain to (and `allowConnectionsWithoutCertificates`), and the subject
//! of a presented client certificate (for MONGODB-X509).

use anyhow::{anyhow, bail, Context, Result};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskCx, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Disabled,
    Allow,
    Prefer,
    Require,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Mode> {
        Ok(match s {
            "disabled" => Mode::Disabled,
            "allowTLS" => Mode::Allow,
            "preferTLS" => Mode::Prefer,
            "requireTLS" => Mode::Require,
            other => bail!("unknown --tls-mode {other:?} (disabled, allowTLS, preferTLS, requireTLS)"),
        })
    }
}

pub struct TlsConfig {
    pub mode: Mode,
    /// Swapped by `rotateCertificates`: new connections use whatever is here when they arrive, open ones keep their session.
    acceptor: std::sync::RwLock<Option<TlsAcceptor>>,
    /// The files the acceptor was built from (read again by `rotateCertificates`).
    files: Files,
}

#[derive(Clone)]
struct Files {
    mode: String,
    certificate_key_file: String,
    ca_file: String,
    allow_without_certificates: bool,
}

pub struct Options<'a> {
    pub mode: &'a str,
    pub certificate_key_file: &'a str,
    pub ca_file: &'a str,
    pub allow_without_certificates: bool,
}

pub fn load(o: &Options) -> Result<TlsConfig> {
    let mode = Mode::parse(o.mode)?;
    let acceptor = build(o)?;
    let files = Files { mode: o.mode.to_string(), certificate_key_file: o.certificate_key_file.to_string(), ca_file: o.ca_file.to_string(), allow_without_certificates: o.allow_without_certificates };
    Ok(TlsConfig { mode, acceptor: std::sync::RwLock::new(acceptor), files })
}

/// Read the certificate files and build the acceptor (None when TLS is disabled).
fn build(o: &Options) -> Result<Option<TlsAcceptor>> {
    let mode = Mode::parse(o.mode)?;
    if mode == Mode::Disabled {
        return Ok(None);
    }
    if o.certificate_key_file.is_empty() {
        bail!("--tls-mode {} needs --tls-certificate-key-file", o.mode);
    }
    let pem = std::fs::read(o.certificate_key_file).with_context(|| format!("reading {}", o.certificate_key_file))?;
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem).collect::<Result<_, _>>().map_err(|e| anyhow!("certificate in {}: {e}", o.certificate_key_file))?;
    if certs.is_empty() {
        bail!("no certificate in {}", o.certificate_key_file);
    }
    let key = PrivateKeyDer::from_pem_slice(&pem).map_err(|e| anyhow!("private key in {}: {e}", o.certificate_key_file))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider)).with_safe_default_protocol_versions()?;
    let builder = if o.ca_file.is_empty() {
        builder.with_no_client_auth()
    } else {
        let ca = std::fs::read(o.ca_file).with_context(|| format!("reading {}", o.ca_file))?;
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(&ca) {
            roots.add(c.map_err(|e| anyhow!("CA certificate in {}: {e}", o.ca_file))?)?;
        }
        let v = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
        let v = if o.allow_without_certificates { v.allow_unauthenticated() } else { v };
        builder.with_client_cert_verifier(v.build()?)
    };
    let cfg = builder.with_single_cert(certs, key)?;
    Ok(Some(TlsAcceptor::from(Arc::new(cfg))))
}

/// A client connection, plain or TLS.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl Stream {
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Stream::Plain(s) => s.peer_addr(),
            Stream::Tls(s) => s.get_ref().0.peer_addr(),
        }
    }
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Stream::Plain(s) => s.local_addr(),
            Stream::Tls(s) => s.get_ref().0.local_addr(),
        }
    }
    pub fn set_nodelay(&self, v: bool) {
        let _ = match self {
            Stream::Plain(s) => s.set_nodelay(v),
            Stream::Tls(s) => s.get_ref().0.set_nodelay(v),
        };
    }
    /// RFC 2253 subject of the client certificate, when one was presented.
    pub fn client_subject(&self) -> Option<String> {
        match self {
            Stream::Plain(_) => None,
            Stream::Tls(s) => s.get_ref().1.peer_certificates().and_then(|c| c.first()).and_then(|der| subject_rfc2253(der.as_ref())),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut TaskCx<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut TaskCx<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskCx<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskCx<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

static INSTALLED: std::sync::OnceLock<Arc<TlsConfig>> = std::sync::OnceLock::new();

/// Make the listener's TLS configuration reachable by `rotateCertificates`.
pub fn install(cfg: Arc<TlsConfig>) {
    let _ = INSTALLED.set(cfg);
}

/// `rotateCertificates`: read the certificate, key and CA files again and use them for every connection accepted from now on
/// (connections already open keep their session). Nothing changes when the files cannot be used: the error says why and the
/// previous certificate stays in force. With TLS disabled there is nothing to rotate (mongod answers ok too).
pub fn rotate() -> Result<()> {
    let Some(cfg) = INSTALLED.get() else { return Ok(()) };
    if cfg.mode == Mode::Disabled {
        return Ok(());
    }
    let f = &cfg.files;
    let acceptor = build(&Options { mode: &f.mode, certificate_key_file: &f.certificate_key_file, ca_file: &f.ca_file, allow_without_certificates: f.allow_without_certificates })?;
    *cfg.acceptor.write().unwrap() = acceptor;
    Ok(())
}

impl TlsConfig {
    /// Accept a connection according to the mode: requireTLS handshakes at once, allowTLS / preferTLS look at the first byte
    /// (0x16 starts a TLS record), disabled never does.
    pub async fn accept(&self, stream: TcpStream) -> Result<Stream> {
        let Some(acceptor) = self.acceptor.read().unwrap().clone() else { return Ok(Stream::Plain(stream)) };
        let tls = match self.mode {
            Mode::Disabled => false,
            Mode::Require => true,
            Mode::Allow | Mode::Prefer => {
                let mut b = [0u8; 1];
                stream.peek(&mut b).await? == 1 && b[0] == 0x16
            }
        };
        if !tls {
            return Ok(Stream::Plain(stream));
        }
        Ok(Stream::Tls(Box::new(acceptor.accept(stream).await?)))
    }
}

/// The subject of a DER certificate in RFC 2253 form (most specific RDN first, `,` between), the way mongod names an x.509 user.
pub fn subject_rfc2253(der: &[u8]) -> Option<String> {
    use x509_parser::prelude::*;
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let mut parts: Vec<String> = Vec::new();
    for rdn in cert.subject().iter_rdn() {
        let mut one: Vec<String> = Vec::new();
        for atv in rdn.iter() {
            let name = short_name(&atv.attr_type().to_id_string());
            let val = atv.as_str().ok()?;
            one.push(format!("{name}={}", escape(val)));
        }
        parts.push(one.join("+"));
    }
    parts.reverse();
    Some(parts.join(","))
}

fn escape(v: &str) -> String {
    let mut out = String::new();
    for (i, c) in v.chars().enumerate() {
        if matches!(c, ',' | '+' | '"' | '\\' | '<' | '>' | ';') || (i == 0 && (c == '#' || c == ' ')) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// RFC 2253 / OpenSSL short names of the attribute types certificates use.
fn short_name(oid: &str) -> String {
    match oid {
        "2.5.4.3" => "CN",
        "2.5.4.4" => "SN",
        "2.5.4.5" => "serialNumber",
        "2.5.4.6" => "C",
        "2.5.4.7" => "L",
        "2.5.4.8" => "ST",
        "2.5.4.9" => "STREET",
        "2.5.4.10" => "O",
        "2.5.4.11" => "OU",
        "2.5.4.12" => "title",
        "2.5.4.42" => "GN",
        "2.5.4.43" => "initials",
        "0.9.2342.19200300.100.1.25" => "DC",
        "0.9.2342.19200300.100.1.1" => "UID",
        "1.2.840.113549.1.9.1" => "emailAddress",
        other => other,
    }
    .to_string()
}
