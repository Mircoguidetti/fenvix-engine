//! Geospatial queries: GeoJSON, legacy coordinate pairs, 2dsphere / 2d indexes.

/// Hidden fields carrying `$geoNear` / `$near` results through a pipeline (`{$meta: "geoNearDistance"}`).
pub const DIST_KEY: &str = "\u{1}geoNearDistance";
pub const POINT_KEY: &str = "\u{1}geoNearPoint";
