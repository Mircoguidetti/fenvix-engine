//! Strategia (d): "livelli geometrici". Le pile (sorted run) restano poche e di taglia crescente con l'età:
//! - quando ci sono almeno 4 L0, diventano una pila nuova (come size-tiered);
//! - una pila si unisce alla successiva (più vecchia) appena è grande almeno 1/T di lei.
//! Così le pile sono circa log_T(taglia / L0) e ogni dato si riscrive circa T/2 volte per livello.
//! Lavora su ogni albero (radice e segmenti) da solo.

use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
use slatedb::Error;
use slatedb::config::CompactorOptions;
use std::collections::HashSet;

pub struct Livelli {
    pub t: f64,
    pub min_l0: usize,
    pub max_attivi: usize,
}

impl CompactionSchedulerSupplier for Livelli {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(Livelli { t: self.t, min_l0: self.min_l0, max_attivi: self.max_attivi })
    }
}

impl CompactionScheduler for Livelli {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let m = state.manifest();
        let mut busy: HashSet<SourceId> = HashSet::new();
        let mut attivi = 0usize;
        let mut next_sr: u32 = 0;
        if let Some(c) = state.compactions() {
            for comp in c.recent_compactions().filter(|c| c.active()) {
                attivi += 1;
                for s in comp.spec().sources() {
                    busy.insert(*s);
                }
                if let Some(d) = comp.spec().destination() {
                    busy.insert(SourceId::SortedRun(d));
                    next_sr = next_sr.max(d + 1);
                }
            }
        }
        let mut alberi: Vec<(bytes::Bytes, Vec<SourceId>, Vec<(u32, u64)>)> = Vec::new();
        let l0 = |v: &std::collections::VecDeque<slatedb::manifest::SsTableView>| v.iter().map(|x| SourceId::SstView(x.id)).collect::<Vec<_>>();
        let srs = |c: &Vec<slatedb::manifest::SortedRun>| c.iter().map(|r| (r.id, r.estimate_size())).collect::<Vec<_>>();
        alberi.push((bytes::Bytes::new(), l0(m.l0()), srs(m.compacted())));
        for s in m.segments() {
            alberi.push((s.prefix().clone(), l0(s.l0()), srs(s.compacted())));
        }
        for (_, _, s) in &alberi {
            for (id, _) in s {
                next_sr = next_sr.max(id + 1);
            }
        }
        let mut out = Vec::new();
        // prima gli alberi con più L0
        alberi.sort_by_key(|a| std::cmp::Reverse(a.1.len()));
        for (prefix, l0s, srs) in alberi {
            if attivi + out.len() >= self.max_attivi {
                break;
            }
            if l0s.len() >= self.min_l0 && l0s.iter().all(|s| !busy.contains(s)) {
                let dst = next_sr;
                next_sr += 1;
                for s in &l0s {
                    busy.insert(*s);
                }
                busy.insert(SourceId::SortedRun(dst));
                out.push(CompactionSpec::for_segment(prefix.clone(), l0s.clone(), dst));
                if attivi + out.len() >= self.max_attivi {
                    break;
                }
            }
            // srs: dal più nuovo al più vecchio
            for i in 0..srs.len().saturating_sub(1) {
                let (a, b) = (srs[i], srs[i + 1]);
                if busy.contains(&SourceId::SortedRun(a.0)) || busy.contains(&SourceId::SortedRun(b.0)) {
                    continue;
                }
                if (a.1 as f64) * self.t >= b.1 as f64 {
                    let dst = a.0.min(b.0);
                    busy.insert(SourceId::SortedRun(a.0));
                    busy.insert(SourceId::SortedRun(b.0));
                    out.push(CompactionSpec::for_segment(prefix.clone(), vec![SourceId::SortedRun(a.0), SourceId::SortedRun(b.0)], dst));
                    if attivi + out.len() >= self.max_attivi {
                        break;
                    }
                }
            }
        }
        out
    }

    /// Copiato da `size_tiered_compaction.rs` di SlateDB 0.17 (righe 297–360): un lavoro passa solo se le sorgenti sono
    /// pile consecutive dell'albero giusto e la destinazione è la pila più vecchia tra le sorgenti.
    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
        let m = state.manifest();
        let albero = if spec.segment().is_empty() {
            Some(ordine_logico(m.l0(), m.compacted()))
        } else {
            m.segments().iter().find(|s| s.prefix() == spec.segment()).map(|s| ordine_logico(s.l0(), s.compacted()))
        };
        valida(albero.as_deref(), spec).map_err(Error::invalid)
    }
}

/// Ordine logico delle sorgenti di un albero: L0 dalla più nuova alla più vecchia, poi le sorted run dalla più nuova (id
/// più alto) alla più vecchia, come in SlateDB.
pub fn ordine_logico(l0: &std::collections::VecDeque<slatedb::manifest::SsTableView>, c: &[slatedb::manifest::SortedRun]) -> Vec<SourceId> {
    l0.iter().map(|v| SourceId::SstView(v.id)).chain(c.iter().map(|r| SourceId::SortedRun(r.id))).collect()
}

/// Il controllo, separato dallo stato di SlateDB per poterlo provare da solo. `albero` = None se il segmento non esiste.
pub fn valida(albero: Option<&[SourceId]>, spec: &CompactionSpec) -> Result<(), String> {
    if spec.is_drain() {
        return Ok(());
    }
    let Some(ordine) = albero else {
        return Err("segmento sconosciuto".into());
    };
    let n = spec.sources().len();
    if n == 0 || n > ordine.len() || !ordine.windows(n).any(|w| w == spec.sources()) {
        return Err(format!("sorgenti non consecutive: {:?} in {:?}", spec.sources(), ordine));
    }
    let min_sr = spec.sources().iter().filter_map(|s| match s {
        SourceId::SortedRun(id) => Some(*id),
        _ => None,
    }).min();
    if let Some(min_sr) = min_sr {
        if spec.destination() != Some(min_sr) {
            return Err(format!("destinazione {:?} non è la pila più vecchia {min_sr}", spec.destination()));
        }
    }
    Ok(())
}

/// Solo per i test: uno schedulatore che propone un lavoro sbagliato (la pila più nuova con la più vecchia, saltando
/// quella in mezzo) e usa il `validate` delle pile a gradini se `con_validate`.
#[cfg(test)]
pub struct Sbagliato {
    pub con_validate: bool,
}

#[cfg(test)]
impl CompactionSchedulerSupplier for Sbagliato {
    fn compaction_scheduler(&self, _o: &CompactorOptions) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(Sbagliato { con_validate: self.con_validate })
    }
}

#[cfg(test)]
impl CompactionScheduler for Sbagliato {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let m = state.manifest();
        if let Some(c) = state.compactions() {
            if c.recent_compactions().any(|c| c.active()) {
                if std::env::var("M29_DEBUG").is_ok() { eprintln!("attivi: {:?}", c.recent_compactions().filter(|c| c.active()).map(|c| format!("{:?}", c.status())).collect::<Vec<_>>()); }
                return vec![];
            }
        }
        let l0: Vec<SourceId> = m.l0().iter().map(|x| SourceId::SstView(x.id)).collect();
        let next = m.compacted().iter().map(|r| r.id + 1).max().unwrap_or(0);
        if !l0.is_empty() {
            return vec![CompactionSpec::new(l0, next)]; // ogni L0 diventa una pila nuova
        }
        let srs: Vec<u32> = m.compacted().iter().map(|r| r.id).collect();
        if std::env::var("M29_DEBUG").is_ok() { eprintln!("propose: srs {:?}", srs); }
        if srs.len() == 3 {
            // sbagliato: nuova + vecchia, saltando quella in mezzo
            return vec![CompactionSpec::new(vec![SourceId::SortedRun(srs[0]), SourceId::SortedRun(srs[2])], srs[2])];
        }
        vec![]
    }
    fn validate(&self, state: &CompactorStateView, spec: &CompactionSpec) -> Result<(), Error> {
        if self.con_validate {
            Livelli { t: 4.0, min_l0: 1, max_attivi: 1 }.validate(state, spec)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn sr(i: u32) -> SourceId {
        SourceId::SortedRun(i)
    }

    #[test]
    fn valida_accetta_e_rifiuta() {
        let a = SourceId::SstView(ulid::Ulid::from_parts(1, 1));
        let b = SourceId::SstView(ulid::Ulid::from_parts(2, 2));
        let albero = vec![b, a, sr(7), sr(5), sr(2)];
        // giusti: tutte le L0 in una pila nuova; due pile vicine nella più vecchia
        assert!(valida(Some(&albero), &CompactionSpec::new(vec![b, a], 8)).is_ok());
        assert!(valida(Some(&albero), &CompactionSpec::new(vec![sr(7), sr(5)], 5)).is_ok());
        assert!(valida(Some(&albero), &CompactionSpec::new(vec![sr(5), sr(2)], 2)).is_ok());
        // sbagliati
        let e = valida(Some(&albero), &CompactionSpec::new(vec![sr(7), sr(2)], 2)).unwrap_err();
        assert!(e.contains("non consecutive"), "{e}");
        let e = valida(Some(&albero), &CompactionSpec::new(vec![sr(5), sr(7)], 5)).unwrap_err();
        assert!(e.contains("non consecutive"), "{e}");
        let e = valida(Some(&albero), &CompactionSpec::new(vec![sr(7), sr(5)], 7)).unwrap_err();
        assert!(e.contains("destinazione"), "{e}");
        let e = valida(Some(&albero), &CompactionSpec::new(vec![a, b], 8)).unwrap_err();
        assert!(e.contains("non consecutive"), "{e}");
        let e = valida(None, &CompactionSpec::new(vec![sr(7)], 7)).unwrap_err();
        assert!(e.contains("segmento"), "{e}");
    }

    /// Senza validate il lavoro sbagliato passa i controlli di SlateDB e una lettura vede un valore vecchio.
    /// Con validate il lavoro è rifiutato e la lettura vede l'ultimo valore.
    async fn prova(con_validate: bool) -> (Vec<u8>, usize) {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut s = slatedb::Settings::default();
        s.compactor_options = None;
        s.l0_sst_size_bytes = 1 << 20;
        let mut co = slatedb::config::CompactorOptions::default();
        co.poll_interval = Duration::from_millis(50);
        let cb = slatedb::CompactorBuilder::new("t", store.clone()).with_options(co).with_scheduler_supplier(Arc::new(Sbagliato { con_validate }));
        let db = slatedb::Db::builder("t", store.clone()).with_settings(s).with_compactor_builder(cb).build().await.unwrap();
        let attendi = |db: &slatedb::Db, n_sr: usize| {
            let db = db.clone();
            async move {
                for _ in 0..200 {
                    let m = db.manifest();
                    if m.l0().is_empty() && m.compacted().len() == n_sr {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                panic!("il lavaggio non arriva a {n_sr} pile");
            }
        };
        for (i, v) in [b"v1", b"v2", b"v3"].iter().enumerate() {
            db.put(b"chiave", *v).await.unwrap();
            db.flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable }).await.unwrap();
            attendi(&db, i + 1).await;
        }
        // il lavoro sbagliato parte appena ci sono 3 pile; aspetto che il lavaggio lo faccia o lo rifiuti
        let mut n = 3;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            n = db.manifest().compacted().len();
            if n != 3 {
                break;
            }
        }
        if !con_validate {
            assert_eq!(n, 2, "il lavoro sbagliato non è stato fatto");
        } else {
            tokio::time::sleep(Duration::from_secs(5)).await;
            n = db.manifest().compacted().len();
        }
        let v = db.get(b"chiave").await.unwrap().unwrap().to_vec();
        db.close().await.unwrap();
        (v, n)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn senza_validate_si_legge_un_valore_vecchio() {
        let (v, n) = prova(false).await;
        assert_eq!(n, 2, "il lavoro sbagliato è stato fatto");
        assert_eq!(v, b"v2", "la lettura vede il valore della pila in mezzo, non l'ultimo");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn con_validate_il_lavoro_sbagliato_e_rifiutato() {
        let (v, n) = prova(true).await;
        assert_eq!(n, 3, "il lavoro sbagliato non è stato fatto");
        assert_eq!(v, b"v3");
    }
}
