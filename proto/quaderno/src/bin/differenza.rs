//! M13 · il record a differenza (prova 2 del revisore di B1). Solo misura, in locale, niente rete.
//!
//! Genera due collezioni "da applicazione" (ordini e utenti, documenti BSON da 1–2 KB con campi realistici, non JSON
//! casuale), le precarica, poi applica una miscela di scritture: 70% `$set` su 1–2 campi, 25% inserimenti,
//! 5% cancellazioni. Per ogni scrittura costruisce il record del quaderno in tre formati:
//!
//! - **A (oggi)**: documento nuovo intero più le voci d'indice cambiate (più guardia dell'indice unico e chiave
//!   dell'ordine naturale), come fa `mongo-server/src/coll.rs`;
//! - **B (differenza)**: per un `$set` solo la differenza in formato oplog v2 di MongoDB (`{u: {...}, s<campo>: {...}}`)
//!   più la posizione della versione di base; le voci d'indice le ricalcola chi applica; restano le guardie
//!   dell'indice unico (servono al capo per la validazione). Inserimento: documento intero. Cancellazione: chiave;
//! - **B2 (differenza + voci d'indice)**: come B, ma le voci d'indice cambiate viaggiano ancora nel record.
//!
//! I record sono codificati con il codec vero del quaderno (`Entry::encode`, `enc_msg`, `enc_req`) e compressi con
//! zstd come fa `node.rs` (`maybe_compress`). Misura anche la CPU di calcolo e di applicazione della differenza, del
//! ricalcolo delle voci d'indice e della compressione.
//!
//! Uso: differenza [--docs 100000] [--ops 200000] [--seed 13] [--out results/m13/x.json]

use bson::{oid::ObjectId, Bson, DateTime, Document};
use bytes::{Bytes, BytesMut};
use quaderno::codec::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeSet, HashMap};
use std::hint::black_box;
use std::time::Instant;

// ------------------------------------------------------------------------------------------------ vocabolari

const FIRST: &[&str] = &[
    "Marco", "Giulia", "Luca", "Francesca", "Alessandro", "Chiara", "Matteo", "Sara", "Lorenzo", "Martina", "Andrea", "Elena",
    "Davide", "Valentina", "Simone", "Federica", "Giorgio", "Alice", "Paolo", "Silvia", "James", "Emma", "Oliver", "Sophie",
    "Lucas", "Mia", "Noah", "Hannah", "Liam", "Laura", "Pierre", "Camille", "Jonas", "Lea", "Mateo", "Lucia", "Daniel",
    "Anna", "Tomás", "Ines",
];
const LAST: &[&str] = &[
    "Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci", "Marino", "Greco", "Bruno", "Gallo",
    "Conti", "De Luca", "Mancini", "Costa", "Giordano", "Rizzo", "Lombardi", "Moretti", "Smith", "Johnson", "Brown", "Müller",
    "Schmidt", "Dubois", "Martin", "García", "Fernández", "López", "Novak", "Kowalski", "Jensen", "Nielsen", "Silva",
    "Santos", "O'Brien", "Murphy", "Weber", "Fischer",
];
const DOMAINS: &[&str] = &["gmail.com", "outlook.com", "yahoo.it", "libero.it", "icloud.com", "hotmail.com", "proton.me", "acme-corp.eu"];
const CITIES: &[(&str, &str, &str)] = &[
    ("Milano", "201", "IT"), ("Roma", "001", "IT"), ("Torino", "101", "IT"), ("Napoli", "801", "IT"), ("Bologna", "401", "IT"),
    ("Firenze", "501", "IT"), ("Genova", "161", "IT"), ("Verona", "371", "IT"), ("Padova", "351", "IT"), ("Bari", "701", "IT"),
    ("Palermo", "901", "IT"), ("Trieste", "341", "IT"), ("Berlin", "101", "DE"), ("München", "803", "DE"), ("Paris", "750", "FR"),
    ("Lyon", "690", "FR"), ("Madrid", "280", "ES"), ("Barcelona", "080", "ES"), ("Lisboa", "110", "PT"), ("Wien", "101", "AT"),
    ("Zürich", "800", "CH"), ("Amsterdam", "101", "NL"), ("Bruxelles", "100", "BE"), ("Dublin", "D02", "IE"),
];
const STREETS: &[&str] = &[
    "Via Roma", "Corso Vittorio Emanuele II", "Via Garibaldi", "Via Mazzini", "Piazza della Repubblica", "Viale Monza",
    "Via Dante Alighieri", "Hauptstraße", "Rue de la Paix", "Calle Mayor", "Via Nazionale", "Corso Buenos Aires",
    "Via Torino", "Viale dei Mille", "Bahnhofstraße", "Avenue Victor Hugo", "Rua Augusta", "Via Cavour",
];
const ADJ: &[&str] = &[
    "Classic", "Premium", "Eco", "Slim", "Ultra", "Compact", "Pro", "Smart", "Vintage", "Organic", "Wireless", "Deluxe",
    "Sport", "Urban", "Travel", "Mini",
];
const NOUN: &[&str] = &[
    "T-Shirt", "Sneakers", "Backpack", "Water Bottle", "Headphones", "Coffee Beans", "Desk Lamp", "Phone Case", "Notebook",
    "Hoodie", "Yoga Mat", "Sunglasses", "Wallet", "Watch", "Charger", "Mug", "Jacket", "Scarf", "Keyboard", "Mouse",
];
const CATEGORY: &[&str] = &["apparel", "footwear", "accessories", "electronics", "home", "food", "sport", "office"];
const ORDER_STATUS: &[&str] = &["pending", "paid", "packed", "shipped", "delivered", "cancelled", "refunded"];
const USER_STATUS: &[&str] = &["active", "active", "active", "inactive", "suspended", "pending_verification"];
const PAY: &[(&str, &str)] = &[("card", "stripe"), ("card", "adyen"), ("paypal", "paypal"), ("bank_transfer", "sepa"), ("klarna", "klarna")];
const SHIP: &[&str] = &["standard", "express", "pickup_point", "same_day"];
const CHANNEL: &[&str] = &["web", "ios", "android", "pos"];
const TAGS: &[&str] = &["gift", "first_order", "b2b", "promo_black_friday", "vip", "fragile", "priority", "newsletter", "returning", "bulk"];
const UA: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 15; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Mobile Safari/537.36",
    "MyShopApp/4.12.3 (iOS 18.1; iPhone15,2)",
];
const UTM: &[&str] = &["google", "facebook", "instagram", "newsletter", "direct", "tiktok", "affiliate"];
const PLANS: &[&str] = &["free", "free", "plus", "pro", "business"];
const LOCALES: &[&str] = &["it-IT", "en-GB", "de-DE", "fr-FR", "es-ES", "en-US"];
const TZ: &[&str] = &["Europe/Rome", "Europe/Berlin", "Europe/Paris", "Europe/Madrid", "Europe/London", "Europe/Lisbon"];
const THEMES: &[&str] = &["light", "dark", "system"];
const WORDS: &[&str] = &[
    "please", "deliver", "after", "before", "the", "door", "leave", "with", "neighbour", "call", "when", "arriving", "ring",
    "bell", "twice", "office", "hours", "only", "gift", "wrap", "thanks", "customer", "asked", "for", "invoice", "company",
    "name", "address", "change", "size", "color", "replace", "item", "damaged", "package", "refund", "requested", "support",
    "ticket", "opened", "closed", "resolved", "waiting", "carrier", "pickup", "tomorrow", "morning", "afternoon", "weekend",
    "urgent", "birthday", "present", "do", "not", "include", "receipt", "second", "floor", "building", "gate", "code",
    "loves", "hiking", "coffee", "design", "travel", "photography", "running", "books", "music", "cooking", "developer",
    "student", "teacher", "freelance", "manager", "based", "in", "and", "a", "of", "to", "my", "is", "on", "at",
];

fn pick<'a, T>(g: &mut StdRng, v: &'a [T]) -> &'a T {
    &v[g.gen_range(0..v.len())]
}

fn text(g: &mut StdRng, words: usize) -> String {
    let mut s = String::new();
    for i in 0..words {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(pick(g, WORDS));
    }
    s
}

/// ObjectId come li fa un driver: secondi, 5 byte casuali del processo (pochi processi), contatore crescente.
struct Oids {
    procs: Vec<[u8; 5]>,
    ctr: u32,
    secs: u32,
}
impl Oids {
    fn new(g: &mut StdRng) -> Oids {
        Oids { procs: (0..6).map(|_| g.gen()).collect(), ctr: g.gen::<u32>() & 0xff_ffff, secs: 1_790_000_000 }
    }
    fn next(&mut self, g: &mut StdRng) -> ObjectId {
        self.ctr = (self.ctr + 1) & 0xff_ffff;
        if g.gen_bool(0.05) {
            self.secs += 1;
        }
        let mut b = [0u8; 12];
        b[..4].copy_from_slice(&self.secs.to_be_bytes());
        b[4..9].copy_from_slice(&self.procs[g.gen_range(0..self.procs.len())]);
        b[9..].copy_from_slice(&self.ctr.to_be_bytes()[1..]);
        ObjectId::from_bytes(b)
    }
}

fn money(g: &mut StdRng, lo: f64, hi: f64) -> f64 {
    (g.gen_range(lo..hi) * 100.0).round() / 100.0
}

struct Gen {
    g: StdRng,
    oids: Oids,
    now_ms: i64,
    order_no: u64,
    user_ids: Vec<ObjectId>,
}

impl Gen {
    fn date_back(&mut self, max_days: i64) -> Bson {
        Bson::DateTime(DateTime::from_millis(self.now_ms - self.g.gen_range(0..max_days * 86_400_000)))
    }
    fn now(&mut self) -> Bson {
        self.now_ms += self.g.gen_range(0..40);
        Bson::DateTime(DateTime::from_millis(self.now_ms))
    }
    fn email(&mut self, f: &str, l: &str) -> String {
        let n: u32 = self.g.gen_range(1..9999);
        let d = *pick(&mut self.g, DOMAINS);
        format!("{}.{}{}@{}", f.to_lowercase(), l.to_lowercase().replace([' ', '\''], ""), n, d)
    }
    fn phone(&mut self) -> String {
        format!("+39 3{:02} {:03} {:04}", self.g.gen_range(20..99), self.g.gen_range(0..999), self.g.gen_range(0..9999))
    }
    fn address(&mut self) -> Document {
        let (c, z, cc) = *pick(&mut self.g, CITIES);
        let mut d = Document::new();
        d.insert("street", format!("{} {}", pick(&mut self.g, STREETS), self.g.gen_range(1..220)));
        d.insert("city", c);
        d.insert("zip", format!("{}{:02}", z, self.g.gen_range(0..99)));
        d.insert("country", cc);
        d
    }
    fn item(&mut self) -> Document {
        let mut d = Document::new();
        d.insert("sku", format!("{}-{:06}", &pick(&mut self.g, CATEGORY)[..3].to_uppercase(), self.g.gen_range(0..1_000_000)));
        d.insert("name", format!("{} {}", pick(&mut self.g, ADJ), pick(&mut self.g, NOUN)));
        d.insert("category", *pick(&mut self.g, CATEGORY));
        d.insert("qty", self.g.gen_range(1..5i32));
        d.insert("unitPrice", money(&mut self.g, 3.0, 300.0));
        d.insert("discount", if self.g.gen_bool(0.3) { money(&mut self.g, 0.5, 20.0) } else { 0.0 });
        d
    }

    /// Ordine da 1–2 KB (obiettivo uniforme).
    fn order(&mut self) -> Document {
        let target = self.g.gen_range(1000..2000usize);
        let id = self.oids.next(&mut self.g);
        let f = *pick(&mut self.g, FIRST);
        let l = *pick(&mut self.g, LAST);
        let created = self.date_back(60);
        self.order_no += 1;
        let mut d = Document::new();
        d.insert("_id", id);
        d.insert("orderNo", format!("ORD-2026-{:08}", self.order_no));
        let uid = if self.user_ids.is_empty() { self.oids.next(&mut self.g) } else { self.user_ids[self.g.gen_range(0..self.user_ids.len())] };
        d.insert("userId", uid);
        d.insert("status", *pick(&mut self.g, ORDER_STATUS));
        d.insert("createdAt", created.clone());
        d.insert("updatedAt", created);
        d.insert("channel", *pick(&mut self.g, CHANNEL));
        d.insert("currency", "EUR");
        let email = self.email(f, l);
        let phone = self.phone();
        d.insert("customer", bson::doc! { "name": format!("{f} {l}"), "email": email, "phone": phone });
        let mut ship = self.address();
        ship.insert("name", format!("{f} {l}"));
        ship.insert("method", *pick(&mut self.g, SHIP));
        ship.insert("trackingNo", Bson::Null);
        d.insert("shipping", ship);
        let (pm, pp) = *pick(&mut self.g, PAY);
        let tx = format!("{:024x}", self.g.gen::<u128>() & ((1u128 << 96) - 1));
        d.insert("payment", bson::doc! { "method": pm, "provider": pp, "status": "authorized", "transactionId": tx });
        let ua = *pick(&mut self.g, UA);
        let utm = *pick(&mut self.g, UTM);
        let ip = format!("{}.{}.{}.{}", self.g.gen_range(2..223), self.g.gen_range(0..255), self.g.gen_range(0..255), self.g.gen_range(1..255));
        d.insert("meta", bson::doc! { "ip": ip, "userAgent": ua, "utmSource": utm });
        let ntags = self.g.gen_range(0..3);
        let tags: Vec<Bson> = (0..ntags).map(|_| Bson::from(*pick(&mut self.g, TAGS))).collect();
        d.insert("tags", tags);
        let mut items = Vec::new();
        let mut sub = 0.0;
        loop {
            let it = self.item();
            sub += it.get_f64("unitPrice").unwrap() * it.get_i32("qty").unwrap() as f64;
            items.push(Bson::Document(it));
            if items.len() >= 14 || (items.len() >= 1 && est(&d) + items.len() * 150 + 120 >= target) {
                break;
            }
        }
        d.insert("items", items);
        let shipc = money(&mut self.g, 0.0, 12.0);
        let tax = (sub * 0.22 * 100.0).round() / 100.0;
        d.insert("totals", bson::doc! { "subtotal": (sub * 100.0).round() / 100.0, "shipping": shipc, "tax": tax, "total": ((sub + shipc + tax) * 100.0).round() / 100.0 });
        // riempie fino all'obiettivo con una nota scritta dal cliente (frasi da vocabolario)
        let have = tv(&d).unwrap().len();
        if have + 30 < target {
            let words = (target - have - 20) / 6;
            let t = text(&mut self.g, words.max(1));
            d.insert("notes", t);
        }
        d
    }

    /// Utente da 1–2 KB (obiettivo uniforme).
    fn user(&mut self) -> Document {
        let target = self.g.gen_range(1000..2000usize);
        let id = self.oids.next(&mut self.g);
        let f = *pick(&mut self.g, FIRST);
        let l = *pick(&mut self.g, LAST);
        let created = self.date_back(700);
        let mut d = Document::new();
        d.insert("_id", id);
        let email = self.email(f, l);
        d.insert("email", email);
        d.insert("name", bson::doc! { "first": f, "last": l });
        d.insert("status", *pick(&mut self.g, USER_STATUS));
        d.insert("plan", *pick(&mut self.g, PLANS));
        d.insert("createdAt", created.clone());
        d.insert("updatedAt", created);
        let ll = self.date_back(30);
        d.insert("lastLoginAt", ll);
        d.insert("locale", *pick(&mut self.g, LOCALES));
        d.insert("timezone", *pick(&mut self.g, TZ));
        let phone = self.phone();
        d.insert("phone", phone);
        let addr = self.address();
        d.insert("address", addr);
        let theme = *pick(&mut self.g, THEMES);
        let lang = &pick(&mut self.g, LOCALES)[..2];
        let (ne, ns, np, nn) = (self.g.gen_bool(0.4), self.g.gen_bool(0.7), self.g.gen_bool(0.2), self.g.gen_bool(0.6));
        d.insert("preferences", bson::doc! { "newsletter": ne, "theme": theme, "language": lang, "notifications": { "email": ns, "sms": np, "push": nn } });
        let logins = self.g.gen_range(1..900i32);
        let orders = self.g.gen_range(0..60i32);
        let spent = money(&mut self.g, 0.0, 4000.0);
        d.insert("stats", bson::doc! { "logins": logins, "orders": orders, "spent": spent });
        let ndev = self.g.gen_range(1..4);
        let mut devs = Vec::new();
        for _ in 0..ndev {
            let ty = *pick(&mut self.g, &["ios", "android", "web"]);
            let os = match ty {
                "ios" => "iOS 18.1",
                "android" => "Android 15",
                _ => "Chrome 130",
            };
            let seen = self.date_back(30);
            let tok = format!("{:032x}{:032x}", self.g.gen::<u128>(), self.g.gen::<u128>());
            let ver = format!("4.{}.{}", self.g.gen_range(0..14), self.g.gen_range(0..9));
            devs.push(Bson::Document(bson::doc! { "type": ty, "os": os, "appVersion": ver, "lastSeenAt": seen, "pushToken": tok }));
        }
        d.insert("devices", devs);
        let ntags = self.g.gen_range(0..4);
        let tags: Vec<Bson> = (0..ntags).map(|_| Bson::from(*pick(&mut self.g, TAGS))).collect();
        d.insert("tags", tags);
        let have = tv(&d).unwrap().len();
        if have + 30 < target {
            let words = (target - have - 20) / 6;
            let t = text(&mut self.g, words.max(1));
            d.insert("bio", t);
        }
        d
    }

    /// Un `$set` realistico su 1–2 campi: restituisce il documento nuovo e il numero di campi toccati.
    fn update(&mut self, coll: Coll, old: &Document) -> (Document, usize) {
        let mut d = old.clone();
        let r: u32 = self.g.gen_range(0..100);
        let now = self.now();
        let set = |d: &mut Document, path: &str, v: Bson| set_path(d, path, v);
        match coll {
            Coll::Orders => {
                if r < 35 {
                    let s = *pick(&mut self.g, ORDER_STATUS);
                    set(&mut d, "status", s.into());
                    set(&mut d, "updatedAt", now);
                    (d, 2)
                } else if r < 50 {
                    let t = format!("1Z{:016X}", self.g.gen::<u64>());
                    set(&mut d, "shipping.trackingNo", t.into());
                    set(&mut d, "updatedAt", now);
                    (d, 2)
                } else if r < 60 {
                    set(&mut d, "payment.status", (*pick(&mut self.g, &["captured", "failed", "refunded", "authorized"])).into());
                    set(&mut d, "payment.paidAt", now);
                    (d, 2)
                } else if r < 70 {
                    let n = d.get_array("items").map(|a| a.len()).unwrap_or(1);
                    let i = self.g.gen_range(0..n);
                    set(&mut d, &format!("items.{i}.qty"), Bson::Int32(self.g.gen_range(1..6)));
                    (d, 1)
                } else if r < 80 {
                    let t = money(&mut self.g, 10.0, 2000.0);
                    set(&mut d, "totals.total", t.into());
                    set(&mut d, "totals.tax", ((t * 0.18 * 100.0).round() / 100.0).into());
                    (d, 2)
                } else if r < 90 {
                    let n = self.g.gen_range(4..20);
                    let t = text(&mut self.g, n);
                    set(&mut d, "notes", t.into());
                    (d, 1)
                } else {
                    // $push di un tag: lo stesso array con un elemento in più
                    let mut tags = d.get_array("tags").cloned().unwrap_or_default();
                    tags.push((*pick(&mut self.g, TAGS)).into());
                    set(&mut d, "tags", tags.into());
                    (d, 1)
                }
            }
            Coll::Users => {
                if r < 35 {
                    set(&mut d, "lastLoginAt", now);
                    let l = d.get_document("stats").ok().and_then(|s| s.get_i32("logins").ok()).unwrap_or(0);
                    set(&mut d, "stats.logins", Bson::Int32(l + 1));
                    (d, 2)
                } else if r < 45 {
                    set(&mut d, "preferences.theme", (*pick(&mut self.g, THEMES)).into());
                    set(&mut d, "updatedAt", now);
                    (d, 2)
                } else if r < 60 {
                    let o = d.get_document("stats").ok().and_then(|s| s.get_i32("orders").ok()).unwrap_or(0);
                    set(&mut d, "stats.orders", Bson::Int32(o + 1));
                    let sp = money(&mut self.g, 10.0, 4000.0);
                    set(&mut d, "stats.spent", sp.into());
                    (d, 2)
                } else if r < 70 {
                    set(&mut d, "status", (*pick(&mut self.g, USER_STATUS)).into());
                    set(&mut d, "updatedAt", now);
                    (d, 2)
                } else if r < 80 {
                    let a = self.address();
                    set(&mut d, "address.city", a.get("city").unwrap().clone());
                    set(&mut d, "address.zip", a.get("zip").unwrap().clone());
                    (d, 2)
                } else if r < 85 {
                    let f = *pick(&mut self.g, FIRST);
                    let l = *pick(&mut self.g, LAST);
                    let e = self.email(f, l);
                    set(&mut d, "email", e.into());
                    set(&mut d, "updatedAt", now);
                    (d, 2)
                } else {
                    set(&mut d, "devices.0.lastSeenAt", now);
                    (d, 1)
                }
            }
        }
    }
}

fn est(d: &Document) -> usize {
    tv(d).map(|v| v.len()).unwrap_or(0)
}

/// `$set` con percorso puntato (campi di sotto-documenti e indici di array), come fa MongoDB.
fn set_path(d: &mut Document, path: &str, v: Bson) {
    let mut parts = path.splitn(2, '.');
    let h = parts.next().unwrap();
    match parts.next() {
        None => {
            d.insert(h, v);
        }
        Some(rest) => {
            let child = d.entry(h.to_string()).or_insert_with(|| Bson::Document(Document::new()));
            set_in(child, rest, v);
        }
    }
}
fn set_in(b: &mut Bson, path: &str, v: Bson) {
    match b {
        Bson::Document(d) => set_path(d, path, v),
        Bson::Array(a) => {
            let mut parts = path.splitn(2, '.');
            let i: usize = parts.next().unwrap().parse().unwrap();
            match parts.next() {
                None => a[i] = v,
                Some(rest) => set_in(&mut a[i], rest, v),
            }
        }
        _ => {}
    }
}

// ------------------------------------------------------------------------------------------------ differenza (oplog v2)

/// Differenza come l'oplog v2 di MongoDB: `u` campi nuovi o cambiati, `d` campi tolti, `s<campo>` differenza dentro
/// un sotto-documento o un array (`a: true`, `u<i>` / `s<i>` per elemento). `None` = documenti uguali.
fn diff(old: &Document, new: &Document) -> Option<Document> {
    let mut u = Document::new();
    let mut del = Document::new();
    let mut subs = Document::new();
    for (k, nv) in new {
        match old.get(k) {
            Some(ov) if ov == nv => {}
            Some(Bson::Document(od)) if matches!(nv, Bson::Document(_)) => {
                if let Bson::Document(nd) = nv {
                    if let Some(s) = diff(od, nd) {
                        subs.insert(format!("s{k}"), s);
                    }
                }
            }
            Some(Bson::Array(oa)) if matches!(nv, Bson::Array(_)) => {
                if let Bson::Array(na) = nv {
                    match diff_array(oa, na) {
                        Some(s) => {
                            subs.insert(format!("s{k}"), s);
                        }
                        None => {
                            u.insert(k.clone(), nv.clone());
                        }
                    }
                }
            }
            _ => {
                u.insert(k.clone(), nv.clone());
            }
        }
    }
    for k in old.keys() {
        if !new.contains_key(k) {
            del.insert(k.clone(), false);
        }
    }
    if u.is_empty() && del.is_empty() && subs.is_empty() {
        return None;
    }
    let mut out = Document::new();
    if !del.is_empty() {
        out.insert("d", del);
    }
    if !u.is_empty() {
        out.insert("u", u);
    }
    out.extend(subs);
    Some(out)
}

/// `None` = conviene riscrivere l'array intero (si è accorciato o è cambiato quasi tutto).
fn diff_array(old: &[Bson], new: &[Bson]) -> Option<Document> {
    if new.len() < old.len() {
        return None;
    }
    let mut out = Document::new();
    out.insert("a", true);
    let mut changed = 0;
    for (i, nv) in new.iter().enumerate() {
        match old.get(i) {
            Some(ov) if ov == nv => {}
            Some(Bson::Document(od)) if matches!(nv, Bson::Document(_)) => {
                if let Bson::Document(nd) = nv {
                    if let Some(s) = diff(od, nd) {
                        out.insert(format!("s{i}"), s);
                        changed += 1;
                    }
                }
            }
            _ => {
                out.insert(format!("u{i}"), nv.clone());
                changed += 1;
            }
        }
    }
    if changed * 2 > new.len().max(1) && new.len() > 2 {
        return None;
    }
    Some(out)
}

fn apply(d: &mut Document, df: &Document) {
    for (k, v) in df {
        match (k.as_str(), v) {
            ("u", Bson::Document(u)) | ("i", Bson::Document(u)) => {
                for (f, x) in u {
                    d.insert(f.clone(), x.clone());
                }
            }
            ("d", Bson::Document(x)) => {
                for f in x.keys() {
                    d.remove(f);
                }
            }
            (s, Bson::Document(sub)) if s.starts_with('s') => {
                let f = &s[1..];
                match d.get_mut(f) {
                    Some(Bson::Document(inner)) => apply(inner, sub),
                    Some(Bson::Array(a)) => apply_array(a, sub),
                    _ => {}
                }
            }
            _ => {}
        }
    }
}
fn apply_array(a: &mut Vec<Bson>, df: &Document) {
    for (k, v) in df {
        if k == "a" {
            continue;
        }
        let i: usize = k[1..].parse().unwrap();
        if k.starts_with('u') {
            if i < a.len() {
                a[i] = v.clone();
            } else {
                while a.len() < i {
                    a.push(Bson::Null);
                }
                a.push(v.clone());
            }
        } else if let (Some(Bson::Document(inner)), Bson::Document(sub)) = (a.get_mut(i), v) {
            apply(inner, sub);
        }
    }
}

// ------------------------------------------------------------------------------------------------ chiavi del motore

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Coll {
    Orders,
    Users,
}
impl Coll {
    fn name(self) -> &'static str {
        match self {
            Coll::Orders => "orders",
            Coll::Users => "users",
        }
    }
    /// 4 indici secondari (con `_id` sono 5, come nelle ipotesi di B1): (nome, campi con verso, unico)
    fn indexes(self) -> &'static [(&'static str, &'static [(&'static str, bool)], bool)] {
        match self {
            Coll::Orders => &[
                ("userId_1", &[("userId", true)], false),
                ("createdAt_-1", &[("createdAt", false)], false),
                ("status_1_createdAt_-1", &[("status", true), ("createdAt", false)], false),
                ("orderNo_1", &[("orderNo", true)], true),
            ],
            Coll::Users => &[
                ("email_1", &[("email", true)], true),
                ("createdAt_-1", &[("createdAt", false)], false),
                ("status_1_lastLoginAt_-1", &[("status", true), ("lastLoginAt", false)], false),
                ("address.city_1", &[("address.city", true)], false),
            ],
        }
    }
}

fn get_path<'a>(d: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut cur = d;
    let mut parts = path.split('.').peekable();
    while let Some(p) = parts.next() {
        let v = cur.get(p)?;
        if parts.peek().is_none() {
            return Some(v);
        }
        match v {
            Bson::Document(x) => cur = x,
            _ => return None,
        }
    }
    None
}

/// Codifica ordinabile semplificata (rango del tipo + byte), stessa forma di `keyenc.rs` per i tipi usati qui.
fn sortable(v: Option<&Bson>, asc: bool) -> Vec<u8> {
    let mut o = Vec::new();
    match v {
        None | Some(Bson::Null) => o.push(0x0a),
        Some(Bson::String(s)) => {
            o.push(0x0f);
            o.extend(s.as_bytes());
            o.push(0);
        }
        Some(Bson::DateTime(t)) => {
            o.push(0x1d);
            o.extend(((t.timestamp_millis() as u64) ^ (1 << 63)).to_be_bytes());
        }
        Some(Bson::ObjectId(x)) => {
            o.push(0x19);
            o.extend(x.bytes());
        }
        Some(Bson::Int32(i)) => {
            o.push(0x0c);
            o.extend(((*i as i64 as u64) ^ (1 << 63)).to_be_bytes());
        }
        Some(other) => {
            o.push(0x30);
            o.extend(tv(&bson::doc! {"v": other.clone()}).unwrap());
        }
    }
    if !asc {
        for b in o.iter_mut() {
            *b = !*b;
        }
    }
    o
}

fn id_enc(d: &Document) -> Vec<u8> {
    sortable(d.get("_id"), true)
}
fn doc_key(c: Coll, id: &[u8]) -> Bytes {
    let mut k = format!("doc/{}/", c.name()).into_bytes();
    k.extend(id);
    Bytes::from(k)
}
fn nat_key(c: Coll, seq: u64) -> Bytes {
    let mut k = format!("nat/{}/", c.name()).into_bytes();
    k.extend(seq.to_be_bytes());
    Bytes::from(k)
}
/// Voci d'indice di un documento: (chiave della voce, è dell'indice unico, chiave della guardia se unico).
fn index_entries(c: Coll, d: &Document, id: &[u8]) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut out = Vec::new();
    for (name, fields, unique) in c.indexes() {
        let mut comps = Vec::new();
        for (f, asc) in fields.iter() {
            comps.extend(sortable(get_path(d, f), *asc));
        }
        let mut k = format!("idx/{}/{}/", c.name(), name).into_bytes();
        k.extend(&comps);
        k.extend(id);
        k.extend((id.len() as u16).to_be_bytes());
        // guardia di unicità (M8): una chiave per valore, valore = id del proprietario
        let guard = unique.then(|| {
            let mut g = format!("uniq/{}/{}/", c.name(), name).into_bytes();
            g.extend(&comps);
            g
        });
        out.push((k, guard));
    }
    out
}
fn doc_val(d: &Document, seq: u64) -> Bytes {
    let mut v = tv(d).unwrap();
    v.push(0x7e);
    v.extend(seq.to_be_bytes());
    Bytes::from(v)
}

// ------------------------------------------------------------------------------------------------ record

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Upd,
    Ins,
    Del,
}

struct Recs {
    op: Op,
    a: Vec<Write>,
    b: Vec<Write>,
    b2: Vec<Write>,
    /// voce dell'oplog di MongoDB per la stessa scrittura (riferimento)
    oplog: Vec<u8>,
    fields: usize,
}

fn w(key: impl Into<Bytes>, kind: Kind, val: impl Into<Bytes>) -> Write {
    Write { key: key.into(), kind, val: val.into() }
}

/// Voci d'indice cambiate tra old e new (cancella le vecchie, scrive le nuove) e guardie cambiate.
fn index_changes(c: Coll, old: Option<&Document>, new: Option<&Document>, id: &[u8], seq: u64) -> (Vec<Write>, Vec<Write>) {
    let eo: Vec<_> = old.map(|d| index_entries(c, d, id)).unwrap_or_default();
    let en: Vec<_> = new.map(|d| index_entries(c, d, id)).unwrap_or_default();
    let so: BTreeSet<&Vec<u8>> = eo.iter().map(|x| &x.0).collect();
    let sn: BTreeSet<&Vec<u8>> = en.iter().map(|x| &x.0).collect();
    let go: BTreeSet<&Vec<u8>> = eo.iter().filter_map(|x| x.1.as_ref()).collect();
    let gn: BTreeSet<&Vec<u8>> = en.iter().filter_map(|x| x.1.as_ref()).collect();
    let mut idx = Vec::new();
    for k in so.difference(&sn) {
        idx.push(w((*k).clone(), Kind::Del, Bytes::new()));
    }
    for k in sn.difference(&so) {
        idx.push(w((*k).clone(), Kind::Put, seq.to_be_bytes().to_vec()));
    }
    let mut guards = Vec::new();
    for k in go.difference(&gn) {
        guards.push(w((*k).clone(), Kind::Del, Bytes::new()));
    }
    for k in gn.difference(&go) {
        guards.push(w((*k).clone(), Kind::Put, id.to_vec()));
    }
    (idx, guards)
}

fn oplog_entry(op: Op, c: Coll, id: &Bson, body: Document, ts: u64, ui: &[u8; 16]) -> Vec<u8> {
    let (s, i) = pos_to_bson_ts(ts);
    let mut e = Document::new();
    e.insert("lsid", bson::doc! { "id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: ui.to_vec() }), "uid": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![7; 32] }) });
    e.insert("txnNumber", 42i64);
    e.insert("op", match op {
        Op::Upd => "u",
        Op::Ins => "i",
        Op::Del => "d",
    });
    e.insert("ns", format!("shop.{}", c.name()));
    e.insert("ui", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: ui.to_vec() }));
    match op {
        Op::Upd => {
            e.insert("o", bson::doc! { "$v": 2, "diff": body });
            e.insert("o2", bson::doc! { "_id": id.clone() });
        }
        Op::Ins => {
            e.insert("o", body);
            e.insert("o2", bson::doc! { "_id": id.clone() });
        }
        Op::Del => {
            e.insert("o", bson::doc! { "_id": id.clone() });
        }
    }
    e.insert("ts", Bson::Timestamp(bson::Timestamp { time: s, increment: i }));
    e.insert("t", 7i64);
    e.insert("v", 2i64);
    e.insert("wall", Bson::DateTime(DateTime::from_millis((ts >> 16) as i64)));
    e.insert("stmtId", 0i32);
    e.insert("prevOpTime", bson::doc! { "ts": Bson::Timestamp(bson::Timestamp { time: 0, increment: 0 }), "t": -1i64 });
    tv(&e).unwrap()
}

// ------------------------------------------------------------------------------------------------ misura

struct Args(HashMap<String, String>);
impl Args {
    fn get<T: std::str::FromStr>(&self, k: &str, d: T) -> T {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
}

fn zstd_len(b: &[u8], lvl: i32) -> usize {
    zstd::bulk::compress(b, lvl).unwrap().len()
}

/// Byte sul filo di un messaggio di replica come in `node.rs::maybe_compress`: frame, compresso solo se conviene e
/// se il corpo supera 512 byte (lvl 0 = spento).
fn wire_len(body: &[u8], lvl: i32) -> usize {
    if lvl == 0 || body.len() < 512 {
        return body.len() + 8;
    }
    let c = zstd_len(body, lvl);
    if c + 5 < body.len() {
        c + 5 + 8
    } else {
        body.len() + 8
    }
}

fn append_body(entries: &[Bytes]) -> Vec<u8> {
    let mut b = BytesMut::new();
    enc_msg(&mut b, &Msg::Append { term: 7, leader: 1, prev_index: 1, prev_term: 7, commit: 1, all_match: 1, seq: 1, entries: entries.to_vec() });
    b.to_vec()
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len().max(1) as f64
}
fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p) as usize]
}

fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut m = HashMap::new();
    let mut i = 0;
    while i + 1 < raw.len() {
        m.insert(raw[i].trim_start_matches("--").to_string(), raw[i + 1].clone());
        i += 2;
    }
    let a = Args(m);
    let ndocs: usize = a.get("docs", 100_000);
    let nops: usize = a.get("ops", 200_000);
    let seed: u64 = a.get("seed", 13);
    let out: String = a.get("out", String::new());
    let p_upd: u32 = a.get("upd", 70);
    let p_ins: u32 = a.get("ins", 25);

    let mut g0 = StdRng::seed_from_u64(seed);
    let oids = Oids::new(&mut g0);
    let mut gen = Gen { g: g0, oids, now_ms: 1_793_000_000_000, order_no: 0, user_ids: Vec::new() };

    // precarico: metà utenti, metà ordini
    let t0 = Instant::now();
    let mut live: HashMap<Coll, Vec<(Document, u64)>> = HashMap::new();
    let mut seq: u64 = 0;
    for _ in 0..ndocs / 2 {
        let u = gen.user();
        gen.user_ids.push(u.get_object_id("_id").unwrap());
        seq += 1;
        live.entry(Coll::Users).or_default().push((u, seq));
    }
    for _ in 0..ndocs / 2 {
        let o = gen.order();
        seq += 1;
        live.entry(Coll::Orders).or_default().push((o, seq));
    }
    let sizes: Vec<f64> = live.values().flatten().map(|(d, _)| est(d) as f64).collect();
    let mut sz = sizes.clone();
    eprintln!("precarico {} documenti in {:.1} s, BSON medio {:.0} B (p5 {:.0}, p95 {:.0})", sizes.len(), t0.elapsed().as_secs_f64(), mean(&sizes), pct(&mut sz, 0.05), pct(&mut sz, 0.95));

    // miscela di scritture
    let ui = [9u8; 16];
    let mut recs: Vec<Recs> = Vec::with_capacity(nops);
    let mut upd_pairs: Vec<(Document, Document)> = Vec::new(); // per le misure di CPU
    let mut pos = pos_make(1_793_000_000_000, 0);
    let mut check_fail = 0usize;
    for _ in 0..nops {
        let c = if gen.g.gen_bool(0.5) { Coll::Orders } else { Coll::Users };
        let r = gen.g.gen_range(0..100u32);
        pos += 1 + gen.g.gen_range(0..3);
        let op = if r < p_upd { Op::Upd } else if r < p_upd + p_ins { Op::Ins } else { Op::Del };
        let v = live.get_mut(&c).unwrap();
        let rec = match op {
            Op::Upd => {
                let j = gen.g.gen_range(0..v.len());
                let (old, s) = v[j].clone();
                let (new, nf) = gen.update(c, &old);
                let id = id_enc(&old);
                let dk = doc_key(c, &id);
                let df = diff(&old, &new).unwrap_or_default();
                // controllo: applicare la differenza al vecchio ridà il nuovo
                let mut chk = old.clone();
                apply(&mut chk, &df);
                if chk != new {
                    check_fail += 1;
                }
                let (idx, guards) = index_changes(c, Some(&old), Some(&new), &id, s);
                let mut a = vec![w(dk.clone(), Kind::Put, doc_val(&new, s))];
                a.extend(idx.iter().cloned());
                a.extend(guards.iter().cloned());
                // valore della differenza: posizione della versione di base (8 byte) + differenza BSON
                let mut dv = pos.to_le_bytes().to_vec();
                dv.extend(tv(&df).unwrap());
                let mut b = vec![w(dk.clone(), Kind::Put, dv)];
                b.extend(guards.iter().cloned());
                let mut b2 = b.clone();
                b2.extend(idx.iter().cloned());
                let oplog = oplog_entry(op, c, old.get("_id").unwrap(), df, pos, &ui);
                if upd_pairs.len() < 50_000 {
                    upd_pairs.push((old, new.clone()));
                }
                v[j].0 = new;
                Recs { op, a, b, b2, oplog, fields: nf }
            }
            Op::Ins => {
                let d = if c == Coll::Users { gen.user() } else { gen.order() };
                if c == Coll::Users {
                    gen.user_ids.push(d.get_object_id("_id").unwrap());
                }
                seq += 1;
                let id = id_enc(&d);
                let dk = doc_key(c, &id);
                let (idx, guards) = index_changes(c, None, Some(&d), &id, seq);
                let mut a = vec![w(dk.clone(), Kind::Put, doc_val(&d, seq)), w(nat_key(c, seq), Kind::Put, id.clone())];
                a.extend(idx.iter().cloned());
                a.extend(guards.iter().cloned());
                // B: la chiave dell'ordine naturale si ricava dal numero in coda al documento, le voci d'indice dal documento
                let mut b = vec![w(dk.clone(), Kind::Put, doc_val(&d, seq))];
                b.extend(guards.iter().cloned());
                let mut b2 = b.clone();
                b2.push(w(nat_key(c, seq), Kind::Put, id.clone()));
                b2.extend(idx.iter().cloned());
                let oplog = oplog_entry(op, c, d.get("_id").unwrap(), d.clone(), pos, &ui);
                v.push((d, seq));
                Recs { op, a, b, b2, oplog, fields: 0 }
            }
            Op::Del => {
                let j = gen.g.gen_range(0..v.len());
                let (old, s) = v.swap_remove(j);
                let id = id_enc(&old);
                let dk = doc_key(c, &id);
                let (idx, guards) = index_changes(c, Some(&old), None, &id, s);
                let mut a = vec![w(dk.clone(), Kind::Del, Bytes::new()), w(nat_key(c, s), Kind::Del, Bytes::new())];
                a.extend(idx.iter().cloned());
                a.extend(guards.iter().cloned());
                let mut b = vec![w(dk.clone(), Kind::Del, Bytes::new())];
                b.extend(guards.iter().cloned());
                let mut b2 = b.clone();
                b2.push(w(nat_key(c, s), Kind::Del, Bytes::new()));
                b2.extend(idx.iter().cloned());
                let oplog = oplog_entry(op, c, old.get("_id").unwrap(), Document::new(), pos, &ui);
                Recs { op, a, b, b2, oplog, fields: 0 }
            }
        };
        recs.push(rec);
    }
    eprintln!("{} scritture generate, controlli differenza falliti: {}", recs.len(), check_fail);

    // codifica con il codec del quaderno
    let session = Some(Session { lsid: Bytes::from(vec![3u8; 16]), txn_number: 42 });
    let db = [5u8; 16];
    let mut entries: [Vec<Bytes>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    let mut reqs: [Vec<CommitReq>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    let mut p = pos_make(1_793_000_000_000, 0);
    for (n, r) in recs.iter().enumerate() {
        p += 1;
        for (f, ws) in [&r.a, &r.b, &r.b2].into_iter().enumerate() {
            let txn: [u8; 16] = rand::random();
            let e = Entry::encode(7, n as u64 + 1, p, Body::Rec { txn, db, session: session.clone(), writes: ws.clone() });
            entries[f].push(e.raw);
            reqs[f].push(CommitReq { txn, db, read_pos: p - 5, mode: Mode::Validated, writes: ws.clone(), reads: vec![], session: session.clone() });
        }
    }
    let names = ["A_oggi", "B_differenza", "B2_differenza_con_indici"];
    let mut res = serde_json::Map::new();
    res.insert("docs".into(), ndocs.into());
    res.insert("ops".into(), nops.into());
    res.insert("seed".into(), seed.into());
    res.insert("doc_bson_mean".into(), mean(&sizes).into());
    res.insert("diff_check_failures".into(), check_fail.into());

    // byte per record, per tipo di operazione
    for (f, name) in names.iter().enumerate() {
        let mut by = serde_json::Map::new();
        for (op, on) in [(Op::Upd, "update"), (Op::Ins, "insert"), (Op::Del, "delete")] {
            let xs: Vec<f64> = recs.iter().zip(&entries[f]).filter(|(r, _)| r.op == op).map(|(_, e)| e.len() as f64).collect();
            by.insert(on.into(), serde_json::json!({"n": xs.len(), "mean_bytes": mean(&xs)}));
        }
        let all: Vec<f64> = entries[f].iter().map(|e| e.len() as f64).collect();
        by.insert("all_mean_bytes".into(), mean(&all).into());
        res.insert(format!("record_{name}"), by.into());
    }
    let ol: Vec<f64> = recs.iter().map(|r| r.oplog.len() as f64).collect();
    let olu: Vec<f64> = recs.iter().filter(|r| r.op == Op::Upd).map(|r| r.oplog.len() as f64).collect();
    let nf: Vec<f64> = recs.iter().filter(|r| r.op == Op::Upd).map(|r| r.fields as f64).collect();
    res.insert("oplog_mongodb".into(), serde_json::json!({"all_mean_bytes": mean(&ol), "update_mean_bytes": mean(&olu)}));
    res.insert("update_fields_mean".into(), mean(&nf).into());
    let diffs: Vec<f64> = recs.iter().filter(|r| r.op == Op::Upd).map(|r| r.b[0].val.len() as f64 - 8.0).collect();
    res.insert("diff_bson_mean_bytes".into(), mean(&diffs).into());

    // replica: messaggi Append con lotti da 1, 16, 32; zstd spento, livello 1, livello -3, livello 3
    let mut repl = serde_json::Map::new();
    for batch in [1usize, 16, 32] {
        for lvl in [0i32, 1, -3, 3] {
            let mut k = serde_json::Map::new();
            for (f, name) in names.iter().enumerate() {
                let mut tot = 0usize;
                for ch in entries[f].chunks(batch) {
                    tot += wire_len(&append_body(ch), lvl);
                }
                k.insert((*name).into(), (tot as f64 / entries[f].len() as f64).into());
            }
            // oplog MongoDB in lotti, stessa compressione (solo riferimento: niente intestazione di messaggio)
            let mut tot = 0usize;
            for ch in recs.chunks(batch) {
                let body: Vec<u8> = ch.iter().flat_map(|r| r.oplog.iter().copied()).collect();
                tot += if lvl == 0 { body.len() } else { zstd_len(&body, lvl).min(body.len()) };
            }
            k.insert("oplog_mongodb".into(), (tot as f64 / recs.len() as f64).into());
            repl.insert(format!("lotto{batch}_zstd{lvl}"), k.into());
        }
    }
    res.insert("replica_bytes_per_record".into(), repl.into());

    // cameriere -> capo: CommitBatch da 16 (oggi senza compressione; con zstd 1 come ipotesi)
    let mut cam = serde_json::Map::new();
    for lvl in [0i32, 1] {
        let mut k = serde_json::Map::new();
        for (f, name) in names.iter().enumerate() {
            let mut tot = 0usize;
            for ch in reqs[f].chunks(16) {
                let body = enc_req(1, &Req::CommitBatch(ch.to_vec()));
                tot += wire_len(&body, lvl);
            }
            k.insert((*name).into(), (tot as f64 / reqs[f].len() as f64).into());
        }
        cam.insert(format!("lotto16_zstd{lvl}"), k.into());
    }
    res.insert("cameriere_capo_bytes_per_record".into(), cam.into());

    // ---------------------------------------------------------------- CPU (un thread)
    let mut cpu = serde_json::Map::new();
    let n = upd_pairs.len();
    // 1. calcolo della differenza dal vecchio e dal nuovo documento (il cameriere li ha già in memoria)
    let t = Instant::now();
    let mut acc = 0usize;
    for (o, nw) in &upd_pairs {
        acc += black_box(diff(o, nw)).map(|d| d.len()).unwrap_or(0);
    }
    cpu.insert("diff_compute_us".into(), (t.elapsed().as_secs_f64() * 1e6 / n as f64).into());
    // 2. serializzazione della differenza vs del documento intero
    let dfs: Vec<Document> = upd_pairs.iter().map(|(o, nw)| diff(o, nw).unwrap_or_default()).collect();
    let t = Instant::now();
    for d in &dfs {
        acc += black_box(tv(d).unwrap()).len();
    }
    cpu.insert("diff_serialize_us".into(), (t.elapsed().as_secs_f64() * 1e6 / n as f64).into());
    let t = Instant::now();
    for (_, nw) in &upd_pairs {
        acc += black_box(tv(nw).unwrap()).len();
    }
    cpu.insert("full_doc_serialize_us".into(), (t.elapsed().as_secs_f64() * 1e6 / n as f64).into());
    // 3. chi applica (lettore, scaricatore, finestra): base in byte -> documento, applica, -> byte
    let bases: Vec<Vec<u8>> = upd_pairs.iter().map(|(o, _)| tv(o).unwrap()).collect();
    let dbytes: Vec<Vec<u8>> = dfs.iter().map(|d| tv(d).unwrap()).collect();
    let t = Instant::now();
    for (b, d) in bases.iter().zip(&dbytes) {
        let mut doc: Document = fs(b).unwrap();
        let df: Document = fs(d).unwrap();
        apply(&mut doc, &df);
        acc += black_box(tv(&doc).unwrap()).len();
    }
    cpu.insert("apply_parse_apply_serialize_us".into(), (t.elapsed().as_secs_f64() * 1e6 / n as f64).into());
    // 3b. solo il parse del documento intero (quello che il lettore fa già oggi per rispondere a una find)
    let t = Instant::now();
    for b in &bases {
        let doc: Document = fs(b).unwrap();
        acc += black_box(doc.len());
    }
    cpu.insert("full_doc_parse_us".into(), (t.elapsed().as_secs_f64() * 1e6 / n as f64).into());
    // 4. catene: k differenze sulla stessa base (documento caldo con più versioni nella finestra)
    for k in [1usize, 4, 16] {
        let reps = 5_000.min(n);
        let t = Instant::now();
        for (i, b) in bases.iter().take(reps).enumerate() {
            let mut doc: Document = fs(b).unwrap();
            for j in 0..k {
                let df: Document = fs(&dbytes[(i + j * 7) % n]).unwrap();
                apply(&mut doc, &df);
            }
            acc += black_box(tv(&doc).unwrap()).len();
        }
        cpu.insert(format!("apply_chain{k}_us"), (t.elapsed().as_secs_f64() * 1e6 / reps as f64).into());
    }
    // 5. ricalcolo delle voci d'indice cambiate (vecchio e nuovo documento, 4 indici, guardie)
    let t = Instant::now();
    for (o, nw) in &upd_pairs {
        let c = if o.contains_key("orderNo") { Coll::Orders } else { Coll::Users };
        let id = id_enc(o);
        let (i1, g1) = index_changes(c, Some(o), Some(nw), &id, 1);
        acc += black_box(i1.len() + g1.len());
    }
    cpu.insert("index_recompute_us".into(), (t.elapsed().as_secs_f64() * 1e6 / n as f64).into());
    // 6. compressione zstd 1 dei lotti da 16, per record
    for (f, name) in names.iter().enumerate() {
        let bodies: Vec<Vec<u8>> = entries[f].chunks(16).map(append_body).collect();
        let t = Instant::now();
        let mut comp = Vec::new();
        for b in &bodies {
            comp.push(zstd::bulk::compress(b, 1).unwrap());
        }
        let ce = t.elapsed().as_secs_f64() * 1e6 / entries[f].len() as f64;
        let t = Instant::now();
        for (c, b) in comp.iter().zip(&bodies) {
            acc += black_box(zstd::bulk::decompress(c, b.len()).unwrap()).len();
        }
        let de = t.elapsed().as_secs_f64() * 1e6 / entries[f].len() as f64;
        cpu.insert(format!("zstd1_lotto16_{name}"), serde_json::json!({"compress_us_per_rec": ce, "decompress_us_per_rec": de}));
    }
    cpu.insert("_sink".into(), (acc % 7).into());
    res.insert("cpu_un_thread".into(), cpu.into());

    let js = serde_json::Value::Object(res);
    let s = serde_json::to_string_pretty(&js).unwrap();
    println!("{s}");
    if !out.is_empty() {
        if let Some(d) = std::path::Path::new(&out).parent() {
            let _ = std::fs::create_dir_all(d);
        }
        std::fs::write(&out, s).unwrap();
    }
}

fn tv(d: &Document) -> bson::error::Result<Vec<u8>> {
    d.to_vec()
}
fn fs(b: &[u8]) -> bson::error::Result<Document> {
    Document::from_reader(b)
}
