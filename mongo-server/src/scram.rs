//! SCRAM-SHA-1 / SCRAM-SHA-256 (RFC 5802 / 7677) as MongoDB uses them: credential documents, the server side of the exchange.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bson::{doc, Document};
use hmac::{Hmac, KeyInit, Mac};
use md5::Digest as _;
use sha1::Sha1;
use sha2::Sha256;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mech {
    Sha1,
    Sha256,
}

impl Mech {
    pub fn parse(s: &str) -> Option<Mech> {
        match s {
            "SCRAM-SHA-1" => Some(Mech::Sha1),
            "SCRAM-SHA-256" => Some(Mech::Sha256),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Mech::Sha1 => "SCRAM-SHA-1",
            Mech::Sha256 => "SCRAM-SHA-256",
        }
    }
    fn iterations(self) -> u32 {
        match self {
            Mech::Sha1 => 10000,
            Mech::Sha256 => 15000,
        }
    }
    fn salt_len(self) -> usize {
        match self {
            Mech::Sha1 => 16,
            Mech::Sha256 => 28,
        }
    }
}

fn hmac(m: Mech, key: &[u8], data: &[u8]) -> Vec<u8> {
    match m {
        Mech::Sha1 => {
            let mut x = <Hmac<Sha1> as KeyInit>::new_from_slice(key).unwrap();
            x.update(data);
            x.finalize().into_bytes().to_vec()
        }
        Mech::Sha256 => {
            let mut x = <Hmac<Sha256> as KeyInit>::new_from_slice(key).unwrap();
            x.update(data);
            x.finalize().into_bytes().to_vec()
        }
    }
}

fn hash(m: Mech, data: &[u8]) -> Vec<u8> {
    match m {
        Mech::Sha1 => Sha1::digest(data).to_vec(),
        Mech::Sha256 => Sha256::digest(data).to_vec(),
    }
}

fn salted(m: Mech, pw: &[u8], salt: &[u8], iters: u32) -> Vec<u8> {
    match m {
        Mech::Sha1 => {
            let mut out = [0u8; 20];
            pbkdf2::pbkdf2_hmac::<Sha1>(pw, salt, iters, &mut out);
            out.to_vec()
        }
        Mech::Sha256 => {
            let mut out = [0u8; 32];
            pbkdf2::pbkdf2_hmac::<Sha256>(pw, salt, iters, &mut out);
            out.to_vec()
        }
    }
}

/// The password string the key derivation runs on: SCRAM-SHA-1 hashes `user:mongo:password` with MD5 first.
fn prepared_password(m: Mech, user: &str, password: &str) -> String {
    match m {
        Mech::Sha1 => md5::Md5::digest(format!("{user}:mongo:{password}").as_bytes()).iter().map(|b| format!("{b:02x}")).collect::<String>(),
        Mech::Sha256 => sasl_prep(password),
    }
}

/// SASLprep (RFC 4013) for the common cases: NFKC, non-ASCII spaces to a space, controls dropped.
pub fn sasl_prep(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    if s.is_ascii() {
        return s.to_string();
    }
    s.chars().map(|c| if c.is_whitespace() && !c.is_ascii() { ' ' } else { c }).filter(|c| !c.is_control() && *c != '\u{200B}' && *c != '\u{00AD}').nfkc().collect()
}

/// Credential sub-document for `mech`, as stored in `admin.system.users`.
pub fn make_credentials(m: Mech, user: &str, password: &str) -> Document {
    let salt: Vec<u8> = (0..m.salt_len()).map(|_| rand::random::<u8>()).collect();
    let iters = m.iterations();
    let salted_pw = salted(m, prepared_password(m, user, password).as_bytes(), &salt, iters);
    let client_key = hmac(m, &salted_pw, b"Client Key");
    let stored = hash(m, &client_key);
    let server = hmac(m, &salted_pw, b"Server Key");
    doc! { "iterationCount": iters as i32, "salt": B64.encode(&salt), "storedKey": B64.encode(&stored), "serverKey": B64.encode(&server) }
}

#[derive(Clone)]
pub struct Creds {
    pub salt_b64: String,
    pub iters: u32,
    pub stored: Vec<u8>,
    pub server: Vec<u8>,
}

impl Creds {
    pub fn from_doc(d: &Document) -> Option<Creds> {
        Some(Creds {
            salt_b64: d.get_str("salt").ok()?.to_string(),
            iters: d.get_i32("iterationCount").ok()? as u32,
            stored: B64.decode(d.get_str("storedKey").ok()?).ok()?,
            server: B64.decode(d.get_str("serverKey").ok()?).ok()?,
        })
    }
}

/// State between the first and the final client message.
pub struct Conv {
    pub mech: Mech,
    pub user: String,
    pub client_first_bare: String,
    pub server_first: String,
    pub nonce: String,
    pub creds: Creds,
}

/// `n,,n=<user>,r=<nonce>` -> (user, client-first-bare, client nonce)
pub fn parse_client_first(msg: &str) -> Option<(String, String, String)> {
    let rest = msg.strip_prefix("n,,").or_else(|| msg.strip_prefix("y,,"))?;
    let mut user = None;
    let mut nonce = None;
    for part in rest.split(',') {
        if let Some(u) = part.strip_prefix("n=") {
            user = Some(u.replace("=2C", ",").replace("=3D", "="));
        } else if let Some(r) = part.strip_prefix("r=") {
            nonce = Some(r.to_string());
        }
    }
    Some((user?, rest.to_string(), nonce?))
}

pub fn server_first(m: Mech, user: &str, client_first_bare: &str, client_nonce: &str, creds: Creds) -> (Conv, String) {
    let snonce = B64.encode((0..18).map(|_| rand::random::<u8>()).collect::<Vec<u8>>());
    let nonce = format!("{client_nonce}{snonce}");
    let msg = format!("r={nonce},s={},i={}", creds.salt_b64, creds.iters);
    (Conv { mech: m, user: user.to_string(), client_first_bare: client_first_bare.to_string(), server_first: msg.clone(), nonce, creds }, msg)
}

/// Verifies the client's proof; returns the `v=` server-final message.
pub fn server_final(c: &Conv, client_final: &str) -> Option<String> {
    let (without_proof, proof_b64) = client_final.rsplit_once(",p=")?;
    let mut nonce_ok = false;
    for part in without_proof.split(',') {
        if let Some(r) = part.strip_prefix("r=") {
            nonce_ok = r == c.nonce;
        }
    }
    if !nonce_ok {
        return None;
    }
    let proof = B64.decode(proof_b64).ok()?;
    let auth_message = format!("{},{},{}", c.client_first_bare, c.server_first, without_proof);
    let sig = hmac(c.mech, &c.creds.stored, auth_message.as_bytes());
    if proof.len() != sig.len() {
        return None;
    }
    let client_key: Vec<u8> = proof.iter().zip(&sig).map(|(a, b)| a ^ b).collect();
    if hash(c.mech, &client_key) != c.creds.stored {
        return None;
    }
    let server_sig = hmac(c.mech, &c.creds.server, auth_message.as_bytes());
    Some(format!("v={}", B64.encode(server_sig)))
}
