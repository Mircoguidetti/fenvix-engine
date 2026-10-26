//! Regular expressions for `$regex`, BSON regex values, `$in`/`$nin` and equality.
//!
//! MongoDB uses PCRE; this uses `fancy-regex` (lookaround and backreferences on top of the `regex`
//! crate). A pattern or option the engine cannot handle returns a MongoDB-shaped error instead of
//! a wrong answer.

use crate::error::MongoError;
use fancy_regex::Regex;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

type R<T> = Result<T, MongoError>;

fn cache() -> &'static Mutex<HashMap<(String, String), Arc<Regex>>> {
    static C: OnceLock<Mutex<HashMap<(String, String), Arc<Regex>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Compile `pattern` with MongoDB `$options` (`i`, `m`, `s`, `x`; `u` is accepted and ignored).
pub fn compile(pattern: &str, options: &str) -> R<Arc<Regex>> {
    let key = (pattern.to_string(), options.to_string());
    if let Some(r) = cache().lock().unwrap().get(&key) {
        return Ok(Arc::clone(r));
    }
    let mut flags = String::new();
    for c in options.chars() {
        match c {
            'i' | 'm' | 's' | 'x' => {
                if !flags.contains(c) {
                    flags.push(c);
                }
            }
            'u' => {}
            other => return Err(MongoError::new(51108, "Location51108", format!("invalid flag in regex options: {other}"))),
        }
    }
    if pattern.contains('\0') {
        return Err(MongoError::new(2, "BadValue", "Regular expression cannot contain an embedded null byte".into()));
    }
    let full = if flags.is_empty() { pattern.to_string() } else { format!("(?{flags}){pattern}") };
    let re = Regex::new(&full).map_err(|e| MongoError::new(51091, "Location51091", format!("Regular expression is invalid: {e}")))?;
    let re = Arc::new(re);
    let mut c = cache().lock().unwrap();
    if c.len() > 512 {
        c.clear();
    }
    c.insert(key, Arc::clone(&re));
    Ok(re)
}

pub fn is_match(re: &Regex, text: &str) -> R<bool> {
    re.is_match(text).map_err(|e| MongoError::new(51156, "Location51156", format!("Regular expression execution failed: {e}")))
}

/// Convenience: compile and match in one go.
pub fn matches_str(pattern: &str, options: &str, text: &str) -> R<bool> {
    is_match(&*compile(pattern, options)?, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_and_lookaround() {
        assert!(matches_str("^ab", "i", "ABc").unwrap());
        assert!(!matches_str("^ab", "", "ABc").unwrap());
        assert!(matches_str("a.b", "s", "a\nb").unwrap());
        assert!(matches_str("^b", "m", "a\nb").unwrap());
        assert!(matches_str("a b", "x", "ab").unwrap());
        assert!(matches_str("foo(?=bar)", "", "foobar").unwrap());
        assert!(matches_str(r"(a)\1", "", "aa").unwrap());
    }

    #[test]
    fn errors_have_mongodb_codes() {
        assert_eq!(compile("a", "z").unwrap_err().code, 51108);
        assert_eq!(compile("(", "").unwrap_err().code, 51091);
    }
}
