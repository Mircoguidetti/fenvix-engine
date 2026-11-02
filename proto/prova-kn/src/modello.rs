//! Modello di latenza di una GET su S3 (la dispensa lontana).
//!
//! tempo di una GET = attesa del primo byte (TTFB) + byte / banda per richiesta
//!
//! - TTFB: lognormale (mediana `mediana_ms`, forma `sigma`), più una mistura facoltativa: con probabilità `lenti_frac`
//!   la richiesta aspetta `lenti_ms` (il caso "un oggetto su cento lentissimo").
//! - banda per richiesta: `banda_mb_s` (MB = 10^6 byte), moltiplicata per un fattore lognormale con forma `sigma_banda`
//!   (una connessione a volte è più lenta).
//!
//! Ogni GET pesca i suoi numeri in modo indipendente dalle altre. È l'ipotesi più favorevole sia per k su n sia per la
//! richiesta doppia: se le lentezze fossero correlate (stesso server S3, stessa partizione) tutte e due guadagnano meno.

use rand::Rng;
use rand_distr::{Distribution, LogNormal};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Modello {
    pub nome: &'static str,
    pub mediana_ms: f64,
    pub sigma: f64,
    pub banda_mb_s: f64,
    pub sigma_banda: f64,
    pub lenti_frac: f64,
    pub lenti_ms: f64,
}

impl Modello {
    /// media 30 ms, p99 100 ms di TTFB (scenario centrale)
    pub const BASE: Modello = Modello { nome: "base", mediana_ms: 25.2, sigma: 0.593, banda_mb_s: 85.0, sigma_banda: 0.25, lenti_frac: 0.0, lenti_ms: 0.0 };
    /// media 25 ms, p99 60 ms (coda leggera)
    pub const LEGGERA: Modello = Modello { nome: "leggera", mediana_ms: 22.9, sigma: 0.413, banda_mb_s: 85.0, sigma_banda: 0.25, lenti_frac: 0.0, lenti_ms: 0.0 };
    /// media 35 ms, p99 200 ms (coda pesante)
    pub const PESANTE: Modello = Modello { nome: "pesante", mediana_ms: 22.5, sigma: 0.9385, banda_mb_s: 85.0, sigma_banda: 0.25, lenti_frac: 0.0, lenti_ms: 0.0 };
    /// base + una richiesta su cento aspetta 1 s
    pub const UNO_SU_CENTO: Modello = Modello { nome: "uno-su-cento", mediana_ms: 25.2, sigma: 0.593, banda_mb_s: 85.0, sigma_banda: 0.25, lenti_frac: 0.01, lenti_ms: 1000.0 };
    /// nessun ritardo (per misurare il costo del banco stesso)
    pub const ZERO: Modello = Modello { nome: "zero", mediana_ms: 0.0, sigma: 0.0, banda_mb_s: 0.0, sigma_banda: 0.0, lenti_frac: 0.0, lenti_ms: 0.0 };

    pub fn per_nome(n: &str) -> Option<Modello> {
        [Self::BASE, Self::LEGGERA, Self::PESANTE, Self::UNO_SU_CENTO, Self::ZERO].into_iter().find(|m| m.nome == n)
    }

    /// Durata in ms di una GET di `byte` byte.
    pub fn campiona<R: Rng>(&self, byte: u64, r: &mut R) -> f64 {
        if self.mediana_ms <= 0.0 {
            return 0.0;
        }
        let ttfb = if self.lenti_frac > 0.0 && r.gen::<f64>() < self.lenti_frac {
            self.lenti_ms
        } else {
            LogNormal::new(self.mediana_ms.ln(), self.sigma).unwrap().sample(r)
        };
        let banda = self.banda_mb_s * LogNormal::new(0.0, self.sigma_banda).unwrap().sample(r);
        ttfb + byte as f64 / (banda * 1e6) * 1e3
    }
}
