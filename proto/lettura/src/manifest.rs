//! Manifest di un gruppo: l'elenco degli SST (run) che contengono i dati scaricati di uno o più database, e fino a
//! che posizione ciascun database è su S3. Si cambia solo con un PUT condizionale (If-Match sull'etag letto, o
//! If-None-Match: * alla creazione): chi perde la gara rilegge e riprova, quindi qualunque worker può scaricare.
//!
//! Gruppo = un database (scarico per database) oppure molti database (oggetto condiviso, variante (b) del passo 5).
//!
//! Chiavi S3 (schema del passo 6): `{h}/{gruppo}/manifest` e `{h}/{gruppo}/sst/{id}.sst`, dove `h` sono 2 cifre
//! esadecimali di un hash del gruppo: 256 prefissi, così il carico si spalma sulle partizioni di S3.

use crate::iface::*;
use crate::sst::hash64;
use crate::store::{Cas, Store};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Run {
    pub id: String,
    pub min: Pos,
    pub max: Pos,
    pub dim: u64,
    pub meta_off: u64,
    pub meta_len: u32,
    pub voci: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DaCancellare {
    pub id: String,
    pub dal_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    pub versione: u64,
    /// db (esadecimale) -> posizione fino a cui i suoi record sono negli SST elencati
    pub scaricato: BTreeMap<String, Pos>,
    /// dal più recente al più vecchio: per ogni database, l'ordine delle run è l'ordine delle posizioni
    pub run: Vec<Run>,
    /// run tolte dalla compattazione: si cancellano dopo un periodo di grazia (lettori con un manifest vecchio)
    pub da_cancellare: Vec<DaCancellare>,
    pub orfani_ms: u64,
    pub ultimo_scarico_ms: u64,
}

impl Manifest {
    pub fn scaricato_db(&self, db: &Db) -> Pos {
        self.scaricato.get(&db_hex(db)).copied().unwrap_or(0)
    }
}

pub fn prefisso(gruppo: &str) -> String {
    format!("{:02x}/{}/", hash64(gruppo.as_bytes()) & 0xff, gruppo)
}
pub fn path_manifest(gruppo: &str) -> String {
    format!("{}manifest", prefisso(gruppo))
}
pub fn path_sst(gruppo: &str, id: &str) -> String {
    format!("{}sst/{}.sst", prefisso(gruppo), id)
}

pub async fn carica(st: &Store, gruppo: &str) -> anyhow::Result<Option<(Manifest, String)>> {
    match st.get(&path_manifest(gruppo)).await? {
        None => Ok(None),
        Some((b, etag)) => Ok(Some((serde_json::from_slice(&b)?, etag.unwrap_or_default()))),
    }
}

pub async fn scrivi(st: &Store, gruppo: &str, m: &Manifest, prev: Option<&str>) -> Cas {
    let b = Bytes::from(serde_json::to_vec(m).unwrap());
    st.put_cond(&path_manifest(gruppo), b, prev).await
}

/// Come si raggruppano i database negli oggetti S3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Raggruppa {
    /// un manifest e i suoi SST per ogni database
    PerDb,
    /// `n` gruppi per hash del database: un oggetto condiviso per gruppo a ogni scarico
    Gruppi(u64),
}

pub fn gruppo_di(db: &Db, r: Raggruppa) -> String {
    match r {
        Raggruppa::PerDb => db_hex(db),
        Raggruppa::Gruppi(n) => format!("g{:05}", hash64(db) % n.max(1)),
    }
}
