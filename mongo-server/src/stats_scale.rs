//! The `scale` option of `collStats`, `dbStats` and `$collStats: {storageStats}`: sizes are reported in units of `scale` bytes.

use crate::cmd::R;
use crate::error::MongoError;
use bson::{Bson, Document};

/// The value of a `scale` option. `db` selects the message of `dbStats` for a value below 1.
pub(crate) fn scale_arg(label: &str, v: Option<&Bson>, db: bool) -> R<i64> {
    let Some(v) = v else { return Ok(1) };
    let n = match v {
        Bson::Int32(i) => *i as i64,
        Bson::Int64(i) => *i,
        Bson::Double(f) => *f as i64,
        Bson::Decimal128(d) => d.to_string().parse::<f64>().map(|f| f as i64).unwrap_or(0),
        other => {
            return Err(MongoError::type_mismatch(format!("BSON field '{label}' is the wrong type '{}', expected types '[long, int, decimal, double']", crate::update::type_name(other))));
        }
    };
    if n < 1 {
        return Err(if db {
            MongoError::bad_value("Scale factor must be greater than zero".into())
        } else {
            MongoError::new(51024, "Location51024", format!("BSON field 'scale' value must be >= 1, actual value '{n}'"))
        });
    }
    Ok(n)
}

fn div(v: &mut Bson, scale: i64) {
    match v {
        Bson::Int32(i) => *i = (*i as i64 / scale) as i32,
        Bson::Int64(i) => *i /= scale,
        Bson::Double(f) => *f = (*f / scale as f64).trunc(),
        _ => {}
    }
}

fn scale_fields(d: &mut Document, fields: &[&str], scale: i64) {
    for f in fields {
        if let Some(v) = d.get_mut(*f) {
            div(v, scale);
        }
    }
}

/// Scale the sizes of a collection's storage statistics.
pub(crate) fn scale_collection(d: &mut Document, scale: i64) {
    if scale == 1 {
        return;
    }
    scale_fields(d, &["size", "storageSize", "freeStorageSize", "totalIndexSize", "totalSize", "maxSize"], scale);
    if let Some(Bson::Document(sizes)) = d.get_mut("indexSizes") {
        for (_, v) in sizes.iter_mut() {
            div(v, scale);
        }
    }
    d.insert("scaleFactor", scale.min(i32::MAX as i64) as i32);
}

/// Scale the sizes of a database's statistics.
pub(crate) fn scale_database(d: &mut Document, scale: i64) {
    if scale == 1 {
        return;
    }
    scale_fields(d, &["dataSize", "storageSize", "indexSize", "totalSize", "freeStorageSize", "indexFreeStorageSize", "totalFreeStorageSize", "fsUsedSize", "fsTotalSize"], scale);
    d.insert("scaleFactor", scale);
}
