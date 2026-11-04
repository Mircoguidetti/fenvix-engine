//! Strategia (d): "livelli geometrici". Le pile (sorted run) restano poche e di taglia crescente con l'età:
//! - quando ci sono almeno 4 L0, diventano una pila nuova (come size-tiered);
//! - una pila si unisce alla successiva (più vecchia) appena è grande almeno 1/T di lei.
//! Così le pile sono circa log_T(taglia / L0) e ogni dato si riscrive circa T/2 volte per livello.
//! Lavora su ogni albero (radice e segmenti) da solo.

use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId};
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
}
