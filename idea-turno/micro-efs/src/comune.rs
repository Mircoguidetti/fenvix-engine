//! Pezzi comuni: argomenti, tempo, percentili, formato del libro, file di segnale.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------- argomenti

pub struct Args {
    vals: HashMap<String, String>,
}

impl Args {
    pub fn parse(raw: &[String]) -> Result<Args, String> {
        let mut vals = HashMap::new();
        let mut i = 0;
        while i < raw.len() {
            let k = &raw[i];
            if !k.starts_with("--") {
                return Err(format!("argomento inatteso: {k}"));
            }
            let key = k.trim_start_matches("--").to_string();
            if i + 1 < raw.len() && !raw[i + 1].starts_with("--") {
                vals.insert(key, raw[i + 1].clone());
                i += 2;
            } else {
                vals.insert(key, "si".to_string());
                i += 1;
            }
        }
        Ok(Args { vals })
    }
    pub fn s(&self, k: &str, def: &str) -> String {
        self.vals.get(k).cloned().unwrap_or_else(|| def.to_string())
    }
    pub fn req(&self, k: &str) -> String {
        match self.vals.get(k) {
            Some(v) => v.clone(),
            None => fail(&format!("manca --{k}")),
        }
    }
    pub fn u(&self, k: &str, def: u64) -> u64 {
        match self.vals.get(k) {
            Some(v) => v.parse().unwrap_or_else(|_| fail(&format!("--{k} non è un numero: {v}"))),
            None => def,
        }
    }
    pub fn f(&self, k: &str, def: f64) -> f64 {
        match self.vals.get(k) {
            Some(v) => v.parse().unwrap_or_else(|_| fail(&format!("--{k} non è un numero: {v}"))),
            None => def,
        }
    }
    pub fn lista_u(&self, k: &str, def: &str) -> Vec<u64> {
        self.s(k, def)
            .split(',')
            .filter(|x| !x.is_empty())
            .map(|x| x.trim().parse().unwrap_or_else(|_| fail(&format!("--{k}: {x}"))))
            .collect()
    }
    pub fn lista_s(&self, k: &str, def: &str) -> Vec<String> {
        self.s(k, def).split(',').filter(|x| !x.is_empty()).map(|x| x.trim().to_string()).collect()
    }
    pub fn si(&self, k: &str, def: bool) -> bool {
        match self.vals.get(k).map(|s| s.as_str()) {
            Some("si") | Some("true") | Some("1") => true,
            Some("no") | Some("false") | Some("0") => false,
            Some(v) => fail(&format!("--{k}: usare si/no, non {v}")),
            None => def,
        }
    }
}

pub fn fail(msg: &str) -> ! {
    eprintln!("errore: {msg}");
    std::process::exit(2);
}

// ---------------------------------------------------------------- tempo

pub fn ora_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

pub fn ora_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn dormi_fino_a(ms: u64) {
    loop {
        let now = ora_ms();
        if now >= ms {
            return;
        }
        let d = ms - now;
        std::thread::sleep(Duration::from_millis(if d > 5 { d - 2 } else { 0 }));
        if d <= 5 {
            while ora_ms() < ms {
                std::hint::spin_loop();
            }
            return;
        }
    }
}

pub fn host_id() -> String {
    let mut buf = [0u8; 128];
    let r = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if r != 0 {
        return "host".into();
    }
    let n = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..n]).replace(['.', '/'], "_")
}

// ---------------------------------------------------------------- percentili

#[derive(Default, Clone)]
pub struct Campioni {
    pub us: Vec<u64>,
}

impl Campioni {
    pub fn add(&mut self, d: Duration) {
        self.us.push(d.as_micros() as u64);
    }
    pub fn unisci(&mut self, o: &Campioni) {
        self.us.extend_from_slice(&o.us);
    }
    /// p in [0,100]; metodo "nearest rank".
    pub fn p(&self, sorted: &[u64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let rank = ((p / 100.0) * sorted.len() as f64 - 1e-9).ceil() as usize;
        let idx = rank.clamp(1, sorted.len()) - 1;
        sorted[idx] as f64 / 1000.0
    }
    /// Restituisce (p50, p99, p99.9, max) in millisecondi.
    pub fn riassunto(&self) -> (f64, f64, f64, f64) {
        let mut s = self.us.clone();
        s.sort_unstable();
        let max = s.last().copied().unwrap_or(0) as f64 / 1000.0;
        (self.p(&s, 50.0), self.p(&s, 99.0), self.p(&s, 99.9), max)
    }
}

// ---------------------------------------------------------------- CRC32 (IEEE)

fn crc_tabella() -> [u32; 256] {
    let mut t = [0u32; 256];
    for i in 0..256u32 {
        let mut c = i;
        for _ in 0..8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
        }
        t[i as usize] = c;
    }
    t
}

pub fn crc32(data: &[u8]) -> u32 {
    use std::sync::OnceLock;
    static T: OnceLock<[u32; 256]> = OnceLock::new();
    let t = T.get_or_init(crc_tabella);
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c = t[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

// ---------------------------------------------------------------- formato del libro
//
// Ogni lotto è un record: intestazione di 24 byte + carico.
//   magic u32 "FVX1" | seq u64 | len u32 (carico) | crc u32 (del carico) | crc_h u32 (dei 20 byte prima)
// Il lettore si ferma al primo record non valido (zeri della preallocazione, coda strappata).

pub const MAGIC: u32 = 0x3158_5646; // "FVX1" little endian
pub const TESTA: usize = 24;

pub fn carico(seq: u64, len: usize) -> Vec<u8> {
    // pseudo-casuale deterministico: xorshift dal seq
    let mut x = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v
}

pub fn record(seq: u64, lotto_byte: usize) -> Vec<u8> {
    let len = lotto_byte.saturating_sub(TESTA).max(1);
    let mut body = carico(seq, len);
    // i primi 8 byte del carico: l'ora di scrittura (µs Unix), per misurare dopo
    // quanto un lettore su un'altra istanza vede il lotto (orologi sincronizzati)
    if len >= 8 {
        body[..8].copy_from_slice(&ora_us().to_le_bytes());
    }
    let mut r = Vec::with_capacity(TESTA + len);
    r.extend_from_slice(&MAGIC.to_le_bytes());
    r.extend_from_slice(&seq.to_le_bytes());
    r.extend_from_slice(&(len as u32).to_le_bytes());
    r.extend_from_slice(&crc32(&body).to_le_bytes());
    let ch = crc32(&r[..20]);
    r.extend_from_slice(&ch.to_le_bytes());
    r.extend_from_slice(&body);
    r
}

/// Controlla un record intero già in memoria; restituisce (seq, byte, ora di scrittura µs).
pub fn valida_record(data: &[u8]) -> Option<(u64, usize, u64)> {
    if data.len() < TESTA {
        return None;
    }
    let h = &data[..TESTA];
    if u32::from_le_bytes(h[0..4].try_into().unwrap()) != MAGIC {
        return None;
    }
    if crc32(&h[..20]) != u32::from_le_bytes(h[20..24].try_into().unwrap()) {
        return None;
    }
    let seq = u64::from_le_bytes(h[4..12].try_into().unwrap());
    let len = u32::from_le_bytes(h[12..16].try_into().unwrap()) as usize;
    if data.len() < TESTA + len {
        return None;
    }
    let body = &data[TESTA..TESTA + len];
    if crc32(body) != u32::from_le_bytes(h[16..20].try_into().unwrap()) {
        return None;
    }
    let t = if len >= 8 { u64::from_le_bytes(body[..8].try_into().unwrap()) } else { 0 };
    Some((seq, TESTA + len, t))
}

/// Lunghezza totale di un record a partire dalla sola intestazione (None se non valida).
pub fn lunghezza_da_testa(h: &[u8]) -> Option<usize> {
    if h.len() < TESTA || u32::from_le_bytes(h[0..4].try_into().unwrap()) != MAGIC {
        return None;
    }
    if crc32(&h[..20]) != u32::from_le_bytes(h[20..24].try_into().unwrap()) {
        return None;
    }
    Some(TESTA + u32::from_le_bytes(h[12..16].try_into().unwrap()) as usize)
}

// ---------------------------------------------------------------- O_DIRECT

pub const O_DIRECT: i32 = libc::O_DIRECT;

/// Buffer allineato a 4096 byte (O_DIRECT lo chiede su molti file system).
pub struct Allineato {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for Allineato {}

impl Allineato {
    pub fn new(len: usize) -> Allineato {
        let len = len.div_ceil(4096).max(1) * 4096;
        let layout = std::alloc::Layout::from_size_align(len, 4096).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Allineato { ptr, len }
    }
    pub fn buf(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for Allineato {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, 4096).unwrap();
        unsafe { std::alloc::dealloc(self.ptr, layout) };
    }
}

pub fn apri_lettura(path: &Path, diretto: bool) -> io::Result<File> {
    let mut oo = OpenOptions::new();
    oo.read(true);
    if diretto {
        oo.custom_flags(O_DIRECT);
    }
    oo.open(path)
}

pub struct Letto {
    pub seq: u64,
    pub offset: u64,
    pub byte: u64,
}

/// Legge un libro dall'inizio e restituisce i record validi, in ordine.
pub fn leggi_libro(path: &Path) -> io::Result<Vec<Letto>> {
    let mut f = File::open(path)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + TESTA <= data.len() {
        let h = &data[off..off + TESTA];
        let magic = u32::from_le_bytes(h[0..4].try_into().unwrap());
        if magic != MAGIC {
            break;
        }
        let ch = u32::from_le_bytes(h[20..24].try_into().unwrap());
        if crc32(&h[..20]) != ch {
            break;
        }
        let seq = u64::from_le_bytes(h[4..12].try_into().unwrap());
        let len = u32::from_le_bytes(h[12..16].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(h[16..20].try_into().unwrap());
        if off + TESTA + len > data.len() {
            break;
        }
        let body = &data[off + TESTA..off + TESTA + len];
        if crc32(body) != crc {
            break;
        }
        out.push(Letto { seq, offset: off as u64, byte: (TESTA + len) as u64 });
        off += TESTA + len;
    }
    Ok(out)
}

// ---------------------------------------------------------------- file

pub const O_DSYNC: i32 = libc::O_DSYNC;

pub fn apri_libro(path: &Path, dsync: bool, prealloca: u64) -> io::Result<File> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let mut oo = OpenOptions::new();
    oo.read(true).write(true).create(true);
    if dsync {
        oo.custom_flags(O_DSYNC);
    }
    let f = oo.open(path)?;
    if prealloca > 0 {
        // zeri scritti davvero (non fallocate): ogni lotto diventa una scrittura "non in coda"
        let blocco = vec![0u8; 1 << 20];
        let mut off = 0u64;
        while off < prealloca {
            let n = ((prealloca - off) as usize).min(blocco.len());
            f.write_all_at(&blocco[..n], off)?;
            off += n as u64;
        }
        f.sync_all()?;
    }
    Ok(f)
}

/// Crea un file che non deve esistere (O_CREAT|O_EXCL) e ci scrive il contenuto.
/// Ok(true) = creato da noi; Ok(false) = esisteva già.
pub fn crea_esclusivo(path: &Path, contenuto: &str) -> io::Result<bool> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut f) => {
            f.write_all(contenuto.as_bytes())?;
            f.sync_all()?;
            Ok(true)
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// La "domanda vera al server": apre per nome il file che segnala la perdita del turno.
/// Va fatta su un mount senza cache delle ricerche (lookupcache=none): così ogni
/// chiamata è un LOOKUP/OPEN al server. Ok(true) = il file esiste (turno perso).
pub fn esiste_dal_server(path: &Path) -> io::Result<bool> {
    match File::open(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn scrivi_testo(path: &Path, s: &str) -> io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(s.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

pub fn aggiungi_riga(path: &Path, riga: &str) -> io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(riga.as_bytes())?;
    f.write_all(b"\n")?;
    Ok(())
}

/// Aspetta che un file di segnale compaia (cercandolo dal server).
pub fn aspetta_segnale(path: &Path, timeout: Duration) -> bool {
    let t0 = Instant::now();
    loop {
        if esiste_dal_server(path).unwrap_or(false) {
            return true;
        }
        if t0.elapsed() > timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn pb(base: &str, parts: &[&str]) -> PathBuf {
    let mut p = PathBuf::from(base);
    for x in parts {
        p.push(x);
    }
    p
}

/// JSON minimo, senza dipendenze: coppie chiave → valore già formattato.
pub fn json(campi: &[(&str, String)]) -> String {
    let mut s = String::from("{");
    for (i, (k, v)) in campi.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("\"{k}\":{v}"));
    }
    s.push('}');
    s
}

pub fn js(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Estrae un campo da una riga JSON prodotta da `json` (numeri o stringhe semplici).
pub fn campo(riga: &str, k: &str) -> Option<String> {
    let pat = format!("\"{k}\":");
    let i = riga.find(&pat)? + pat.len();
    let rest = &riga[i..];
    if let Some(r) = rest.strip_prefix('"') {
        let e = r.find('"')?;
        Some(r[..e].to_string())
    } else {
        let e = rest.find([',', '}']).unwrap_or(rest.len());
        Some(rest[..e].trim().to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn crc_noto() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn libro_ida_e_ritorno() {
        let dir = std::env::temp_dir().join(format!("microefs-test-{}", std::process::id()));
        let p = dir.join("l.log");
        let f = apri_libro(&p, false, 64 * 1024).unwrap();
        let mut off = 0u64;
        for s in 1..=10u64 {
            let r = record(s, 1000);
            f.write_all_at(&r, off).unwrap();
            off += r.len() as u64;
        }
        f.sync_all().unwrap();
        let l = leggi_libro(&p).unwrap();
        assert_eq!(l.len(), 10);
        assert_eq!(l[9].seq, 10);
        // un record rovinato ferma la lettura
        f.write_all_at(&[0xFF], 5 * 1000 + 30).unwrap();
        let l = leggi_libro(&p).unwrap();
        assert_eq!(l.len(), 5);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn esclusivo_un_solo_vincitore() {
        let dir = std::env::temp_dir().join(format!("microefs-excl-{}", std::process::id()));
        let p = dir.join("turni/d/5");
        assert!(crea_esclusivo(&p, "a").unwrap());
        assert!(!crea_esclusivo(&p, "b").unwrap());
        assert_eq!(fs::read_to_string(&p).unwrap(), "a");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn percentili() {
        let mut c = Campioni::default();
        for i in 1..=1000u64 {
            c.us.push(i * 1000);
        }
        let (p50, p99, p999, max) = c.riassunto();
        assert_eq!((p50, p99, p999, max), (500.0, 990.0, 999.0, 1000.0));
    }

    #[test]
    fn json_campo() {
        let r = json(&[("a", "1".into()), ("b", js("x y")), ("c", "2.5".into())]);
        assert_eq!(campo(&r, "a").unwrap(), "1");
        assert_eq!(campo(&r, "b").unwrap(), "x y");
        assert_eq!(campo(&r, "c").unwrap(), "2.5");
    }
}
