//! Geo query conditions (`$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`), their evaluation on documents, the
//! cell coverings behind 2dsphere / 2d index entries and scans, and the `$geoNear` stage helpers.

use crate::error::MongoError;
use crate::geo::*;
use bson::{Bson, Document};
use s2::cell::Cell;
use s2::cellid::CellID;
use std::collections::{BTreeMap, BinaryHeap};

type R<T> = Result<T, MongoError>;

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

fn num(b: &Bson) -> Option<f64> {
    match b {
        Bson::Double(f) => Some(*f),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------------
// Conditions
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Planar {
    Box([f64; 2], [f64; 2]),
    Center([f64; 2], f64),
    Polygon(Vec<[f64; 2]>),
}

#[derive(Clone, Debug)]
pub enum Region {
    /// `$geometry` polygon(s).
    Sphere(Vec<Poly>),
    CenterSphere(V, f64),
    Planar(Planar),
}

#[derive(Clone, Debug)]
pub struct Near {
    /// `$nearSphere`, or a GeoJSON `$geometry`: spherical distances.
    pub spherical: bool,
    /// Distances are meters (GeoJSON query) rather than radians / coordinate units.
    pub meters: bool,
    pub center_xy: [f64; 2],
    pub center: V,
    pub min: f64,
    pub max: f64,
    pub from_geometry: bool,
}

#[derive(Clone, Debug)]
pub enum Cond {
    Within(Region),
    Intersects(Shape),
    Near(Near),
}

pub fn is_geo_key(k: &str) -> bool {
    matches!(k, "$geoWithin" | "$geoIntersects" | "$near" | "$nearSphere" | "$within" | "$geoNear")
}

fn point2(b: &Bson) -> R<[f64; 2]> {
    legacy_pair(b).ok_or_else(|| bad("Point must be an array or object"))
}

fn parse_within(arg: &Bson) -> R<Region> {
    let Bson::Document(d) = arg else { return Err(bad("$geoWithin not supported with provided geometry")) };
    let mut found: Option<Region> = None;
    for (k, v) in d {
        let r = match k.as_str() {
            "$geometry" => {
                let Bson::Document(g) = v else { return Err(bad("$geometry must be an object")) };
                match parse_geojson(g).map_err(bad)? {
                    Shape::Polygon(p) => Region::Sphere(vec![p]),
                    Shape::MultiPolygon(ps) => Region::Sphere(ps),
                    _ => return Err(bad(format!("$within not supported with provided geometry: {{ $geoWithin: {} }}", crate::update::fmt_doc(d)))),
                }
            }
            "$box" => {
                let Bson::Array(a) = v else { return Err(bad("$box must be an array")) };
                if a.len() < 2 {
                    return Err(bad("Only 2 points allowed for $box"));
                }
                let (p, q) = (point2(&a[0])?, point2(&a[1])?);
                Region::Planar(Planar::Box([p[0].min(q[0]), p[1].min(q[1])], [p[0].max(q[0]), p[1].max(q[1])]))
            }
            "$center" | "$centerSphere" => {
                let Bson::Array(a) = v else { return Err(bad(format!("{k} must be an array"))) };
                if a.len() != 2 {
                    return Err(bad(format!("{k} requires a center and a radius")));
                }
                let c = point2(&a[0])?;
                let r = num(&a[1]).ok_or_else(|| bad("radius must be a number"))?;
                if r < 0.0 {
                    return Err(bad("radius must be non-negative"));
                }
                if k == "$center" {
                    Region::Planar(Planar::Center(c, r))
                } else {
                    if c[0].abs() > 180.0 || c[1].abs() > 90.0 {
                        return Err(bad(format!("Longitude/latitude is out of bounds, lng: {} lat: {}", fmt_num(c[0]), fmt_num(c[1]))));
                    }
                    Region::CenterSphere(from_lnglat(c[0], c[1]), r)
                }
            }
            "$polygon" => {
                let Bson::Array(a) = v else { return Err(bad("$polygon must be an array")) };
                let pts: Vec<[f64; 2]> = a.iter().map(point2).collect::<R<_>>()?;
                if pts.len() < 3 {
                    return Err(bad("Polygon must have at least 3 points"));
                }
                Region::Planar(Planar::Polygon(pts))
            }
            other => return Err(bad(format!("unknown geo specifier: {other}"))),
        };
        if found.is_some() {
            return Err(bad("Only one geo region allowed"));
        }
        found = Some(r);
    }
    found.ok_or_else(|| bad("geo query doesn't have any geometry"))
}

fn parse_near(name: &str, arg: &Bson, max: Option<&Bson>, min: Option<&Bson>) -> R<Near> {
    let sphere_op = name == "$nearSphere";
    let mut max_v = max.cloned();
    let mut min_v = min.cloned();
    let (center_xy, center, from_geometry): ([f64; 2], V, bool) = match arg {
        Bson::Document(d) if d.contains_key("$geometry") || d.contains_key("$maxDistance") || d.contains_key("$minDistance") => {
            if d.contains_key("$geometry") {
                for (nm, o) in [("$maxDistance", max), ("$minDistance", min)] {
                    if let Some(v) = o {
                        return Err(bad(format!("geo near accepts just one argument when querying for a GeoJSON point. Extra field found: {nm}: {}", crate::update::fmt_bson(v))));
                    }
                }
            }
            if let Some(m) = d.get("$maxDistance") {
                max_v = Some(m.clone());
            }
            if let Some(m) = d.get("$minDistance") {
                min_v = Some(m.clone());
            }
            let g = match d.get("$geometry") {
                Some(Bson::Document(g)) => g,
                _ => return Err(bad("$geometry is required for geo near query")),
            };
            match parse_geojson(g) {
                Ok(Shape::Point(p)) => {
                    let (lng, lat) = to_lnglat(&p);
                    ([lng, lat], p, true)
                }
                Ok(_) => return Err(bad(format!("invalid point in geo near query $geometry argument: {}  Point must be a GeoJSON Point", crate::update::fmt_doc(g)))),
                Err(e) => return Err(bad(format!("invalid point in geo near query $geometry argument: {}  {e}", crate::update::fmt_doc(g)))),
            }
        }
        other => {
            let xy = legacy_pair(other).ok_or_else(|| bad("near must be first in: { $near: [x, y] }"))?;
            let in_range = xy[0].abs() <= 180.0 && xy[1].abs() <= 90.0;
            if sphere_op && !in_range {
                return Err(bad(format!("Longitude/latitude is out of bounds, lng: {} lat: {}", fmt_num(xy[0]), fmt_num(xy[1]))));
            }
            let v = if in_range { from_lnglat(xy[0], xy[1]) } else { [0.0, 0.0, 1.0] };
            (xy, v, false)
        }
    };
    let dist = |b: Option<Bson>, what: &str| -> R<Option<f64>> {
        match b {
            None => Ok(None),
            Some(v) => {
                let n = num(&v).ok_or_else(|| bad(format!("{what} must be a number")))?;
                if n < 0.0 {
                    return Err(bad(format!("{what} must be non-negative")));
                }
                Ok(Some(n))
            }
        }
    };
    let max = dist(max_v, "$maxDistance")?.unwrap_or(f64::INFINITY);
    let min = dist(min_v, "$minDistance")?.unwrap_or(0.0);
    Ok(Near { spherical: sphere_op || from_geometry, meters: from_geometry, center_xy, center, min, max, from_geometry })
}

/// The geo conditions of a field's operator document (`{$geoWithin: ..}`, `{$near: .., $maxDistance: ..}`), if any.
pub fn parse_field_conds(d: &Document) -> R<Vec<Cond>> {
    let mut out = Vec::new();
    if d.keys().filter(|k| matches!(k.as_str(), "$near" | "$nearSphere")).count() > 1 {
        return Err(bad("Too many geoNear expressions"));
    }
    if d.keys().any(|k| matches!(k.as_str(), "$near" | "$nearSphere")) {
        if let Some(other) = d.keys().find(|k| matches!(k.as_str(), "$geoWithin" | "$within" | "$geoIntersects")) {
            return Err(MongoError::new(34413, "Location34413", format!("invalid argument in geo near query: {other}")));
        }
    }
    for (k, v) in d {
        match k.as_str() {
            "$geoWithin" | "$within" => out.push(Cond::Within(parse_within(v)?)),
            "$geoIntersects" => {
                let g = match v {
                    Bson::Document(g) => g.get("$geometry"),
                    _ => None,
                };
                match g {
                    Some(Bson::Document(g)) => out.push(Cond::Intersects(parse_geojson(g).map_err(bad)?)),
                    _ => return Err(bad(format!("$geoIntersect not supported with provided geometry: {{ $geoIntersects: {} }}", crate::update::fmt_bson(v)))),
                }
            }
            "$near" | "$nearSphere" => out.push(Cond::Near(parse_near(k, v, d.get("$maxDistance"), d.get("$minDistance"))?)),
            _ => {}
        }
    }
    Ok(out)
}

pub fn has_geo_cond(d: &Document) -> bool {
    d.keys().any(|k| is_geo_key(k))
}

// ---------------------------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------------------------

fn planar_contains(p: &Planar, xy: [f64; 2]) -> bool {
    const E: f64 = 1e-11;
    match p {
        Planar::Box(lo, hi) => xy[0] >= lo[0] && xy[0] <= hi[0] && xy[1] >= lo[1] && xy[1] <= hi[1],
        Planar::Center(c, r) => ((xy[0] - c[0]).powi(2) + (xy[1] - c[1]).powi(2)).sqrt() <= *r,
        Planar::Polygon(pts) => {
            let n = pts.len();
            let mut inside = false;
            for i in 0..n {
                let (a, b) = (pts[i], pts[(i + 1) % n]);
                // on the boundary
                let cross = (b[0] - a[0]) * (xy[1] - a[1]) - (b[1] - a[1]) * (xy[0] - a[0]);
                if cross.abs() < E && xy[0] >= a[0].min(b[0]) - E && xy[0] <= a[0].max(b[0]) + E && xy[1] >= a[1].min(b[1]) - E && xy[1] <= a[1].max(b[1]) + E {
                    return true;
                }
                if (a[1] > xy[1]) != (b[1] > xy[1]) && xy[0] < (b[0] - a[0]) * (xy[1] - a[1]) / (b[1] - a[1]) + a[0] {
                    inside = !inside;
                }
            }
            inside
        }
    }
}

/// Planar coordinates of an element that is a point.
pub fn elem_xy(e: &Elem) -> Option<[f64; 2]> {
    match e {
        Elem::Legacy(p) => Some(*p),
        Elem::Geo(Shape::Point(v)) => {
            let (lng, lat) = to_lnglat(v);
            Some([lng, lat])
        }
        _ => None,
    }
}

fn within_elem(r: &Region, e: &Elem) -> bool {
    match r {
        Region::Sphere(polys) => elem_shape(e).map(|s| within(&s, polys)).unwrap_or(false),
        Region::CenterSphere(c, rad) => elem_point(e).map(|p| angle(&p, c) <= *rad + 1e-12).unwrap_or(false),
        Region::Planar(p) => elem_xy(e).map(|xy| planar_contains(p, xy)).unwrap_or(false),
    }
}

/// Distance of the nearest element from the query point, in the units of the condition, with the element.
pub fn near_distance(n: &Near, elems: &[(Elem, Bson)]) -> Option<(f64, Bson)> {
    let mut best: Option<(f64, Bson)> = None;
    for (e, raw) in elems {
        let d = if n.spherical {
            let s = elem_shape(e)?;
            let a = s.dist_point(&n.center);
            // the reference derives radians from meters
            if n.meters {
                a * EARTH_RADIUS_M
            } else {
                a * EARTH_RADIUS_M / EARTH_RADIUS_M
            }
        } else {
            match elem_xy(e) {
                Some(xy) => ((xy[0] - n.center_xy[0]).powi(2) + (xy[1] - n.center_xy[1]).powi(2)).sqrt(),
                None => continue,
            }
        };
        if best.as_ref().map(|(b, _)| d < *b).unwrap_or(true) {
            let loc = raw.clone();
            best = Some((d, loc));
        }
    }
    best
}

/// Index-order key of a matched location, to order documents at equal distance like the index scan does.
pub fn tie_key(spec: &GeoSpec, raw: &Bson) -> u64 {
    if spec.sphere {
        match elements(raw).first() {
            Some(e) => elem_point(e).map(|p| CellID::from(&s2_point(&p)).0).unwrap_or(0),
            None => 0,
        }
    } else {
        legacy_pair(raw).map(|p| spec.hash2d(p[0], p[1])).unwrap_or(0)
    }
}

/// All geo conditions of `d` against the values of a field.
pub fn match_field(vals: &[&Bson], d: &Document) -> R<bool> {
    let conds = parse_field_conds(d)?;
    let raw: Vec<(Elem, Bson)> = vals.iter().flat_map(|v| elements_raw(v)).collect();
    let elems: Vec<Elem> = raw.iter().map(|(e, _)| e.clone()).collect();
    for c in &conds {
        let ok = match c {
            Cond::Within(r) => elems.iter().any(|e| within_elem(r, e)),
            Cond::Intersects(s) => elems.iter().any(|e| elem_shape(e).map(|x| intersects(&x, s)).unwrap_or(false)),
            Cond::Near(n) => match near_distance(n, &raw) {
                Some((dist, _)) => dist >= n.min && dist <= n.max,
                None => false,
            },
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The `$near` condition of a filter and its field path (top level or inside `$and`).
pub fn find_near(filter: &Document) -> R<Option<(String, Near)>> {
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("$and", Bson::Array(a)) => {
                for s in a {
                    if let Bson::Document(sd) = s {
                        if let Some(x) = find_near(sd)? {
                            return Ok(Some(x));
                        }
                    }
                }
            }
            (k2, Bson::Document(d)) if !k2.starts_with('$') && d.keys().any(|x| x == "$near" || x == "$nearSphere") => {
                for c in parse_field_conds(d)? {
                    if let Cond::Near(n) = c {
                        return Ok(Some((k.clone(), n)));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Does the filter use a geo operator anywhere (including inside `$or` / `$nor`, where `$near` is an error)?
pub fn filter_has_geo(filter: &Document) -> bool {
    contains_near(filter) || filter.iter().any(|(k, v)| match (k.as_str(), v) {
        ("$and" | "$or" | "$nor", Bson::Array(a)) => a.iter().any(|s| matches!(s, Bson::Document(d) if filter_has_geo(d))),
        (_, Bson::Document(d)) => d.keys().any(|x| is_geo_key(x)) || d.values().any(|x| matches!(x, Bson::Document(dd) if dd.keys().any(|y| matches!(y.as_str(), "$not")))),
        _ => false,
    })
}

/// Validate the geo conditions of a filter before running it and reject placements MongoDB rejects.
pub fn validate_filter(filter: &Document, top: bool) -> R<()> {
    if top && count_near(filter) > 1 {
        return Err(bad("Too many geoNear expressions"));
    }
    validate_conds(filter, top)
}

/// Number of `$near` / `$nearSphere` conditions in the top-level / `$and` structure of a filter.
fn count_near(filter: &Document) -> usize {
    filter
        .iter()
        .map(|(k, v)| match (k.as_str(), v) {
            ("$and", Bson::Array(a)) => a.iter().map(|s| if let Bson::Document(d) = s { count_near(d) } else { 0 }).sum(),
            (_, Bson::Document(d)) if !k.starts_with('$') => d.keys().filter(|x| matches!(x.as_str(), "$near" | "$nearSphere")).count(),
            _ => 0,
        })
        .sum()
}

/// Does any `$near` / `$nearSphere` key appear anywhere inside the value?
pub fn contains_near(d: &Document) -> bool {
    d.iter().any(|(k, v)| {
        matches!(k.as_str(), "$near" | "$nearSphere")
            || match v {
                Bson::Document(x) => contains_near(x),
                Bson::Array(a) => a.iter().any(|y| matches!(y, Bson::Document(x) if contains_near(x))),
                _ => false,
            }
    })
}

fn validate_conds(filter: &Document, top: bool) -> R<()> {
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("$and", Bson::Array(a)) => {
                for s in a {
                    if let Bson::Document(sd) = s {
                        validate_conds(sd, top)?;
                    }
                }
            }
            ("$or" | "$nor", Bson::Array(a)) => {
                for s in a {
                    if let Bson::Document(sd) = s {
                        validate_conds(sd, false)?;
                    }
                }
            }
            (k2, Bson::Document(d)) if !k2.starts_with('$') && has_geo_cond(d) => {
                let conds = parse_field_conds(d)?;
                if !top && conds.iter().any(|c| matches!(c, Cond::Near(_))) {
                    return Err(bad("geo $near must be top-level expr"));
                }
            }
            (k2, Bson::Document(d)) if !k2.starts_with('$') && (matches!(d.get("$elemMatch"), Some(Bson::Document(em)) if contains_near(em)) || matches!(d.get("$not"), Some(Bson::Document(nt)) if contains_near(nt))) => {
                return Err(bad("geo $near must be top-level expr"));
            }
            _ => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Cell coverings
// ---------------------------------------------------------------------------------------------

fn cell_radius(c: &Cell) -> f64 {
    let ctr = c.center();
    let ctr = [ctr.0.x, ctr.0.y, ctr.0.z];
    c.vertices().iter().map(|v| angle(&ctr, &[v.0.x, v.0.y, v.0.z])).fold(0.0, f64::max)
}

fn cell_center(c: &Cell) -> V {
    let p = c.center();
    [p.0.x, p.0.y, p.0.z]
}

/// How far inside the region `p` is (distance to its boundary), if the shape is an area and contains `p`.
fn margin(shape: &Shape, p: &V) -> Option<f64> {
    match shape {
        Shape::Polygon(pg) if pg.contains(p) => Some(pg.boundary_distance(p)),
        Shape::MultiPolygon(pgs) => pgs.iter().filter(|pg| pg.contains(p)).map(|pg| pg.boundary_distance(p)).fold(None, |a, b| Some(a.map_or(b, |x: f64| x.max(b)))),
        Shape::Cap(c, r) => {
            let a = angle(p, c);
            if a <= *r {
                Some(r - a)
            } else {
                None
            }
        }
        Shape::Collection(ss) => ss.iter().filter_map(|s| margin(s, p)).fold(None, |a, b| Some(a.map_or(b, |x: f64| x.max(b)))),
        _ => None,
    }
}

#[derive(PartialEq, Eq)]
struct ByLevel(u64, u64); // (level, id) ordered so the lowest level pops first

impl Ord for ByLevel {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.cmp(&self.0).then(o.1.cmp(&self.1))
    }
}
impl PartialOrd for ByLevel {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

/// Cells that together cover `shape` (at most `max_cells` once the minimum level is reached, levels in
/// `[min_level, max_level]`).
pub fn cover(shape: &Shape, max_cells: usize, min_level: u64, max_level: u64) -> Vec<CellID> {
    let may = |id: &CellID| -> bool {
        let c = Cell::from(id);
        shape.dist_point(&cell_center(&c)) <= cell_radius(&c) + 1e-12
    };
    let mut heap: BinaryHeap<ByLevel> = BinaryHeap::new();
    for f in 0..6u64 {
        let id = CellID::from_face(f);
        if may(&id) {
            heap.push(ByLevel(0, id.0));
        }
    }
    let mut done: Vec<CellID> = Vec::new();
    while let Some(ByLevel(level, raw)) = heap.pop() {
        let id = CellID(raw);
        let c = Cell::from(&id);
        let inside = level >= min_level && margin(shape, &cell_center(&c)).map(|m| m >= cell_radius(&c)).unwrap_or(false);
        if inside || level >= max_level {
            done.push(id);
            continue;
        }
        let kids: Vec<CellID> = id.children().into_iter().filter(|k| may(k)).collect();
        if level >= min_level && done.len() + heap.len() + kids.len() > max_cells {
            done.push(id);
            while let Some(ByLevel(_, r)) = heap.pop() {
                done.push(CellID(r));
            }
            break;
        }
        for k in kids {
            heap.push(ByLevel(level + 1, k.0));
        }
    }
    done.sort_by_key(|c| c.0);
    done
}

/// Hex form used in index keys: fixed width, so key order equals cell id order.
pub fn token(id: u64) -> String {
    format!("{id:016x}")
}

// ---------------------------------------------------------------------------------------------
// Index specification and entries
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct GeoSpec {
    pub sphere: bool,
    pub field: String,
    pub prefix: Vec<(String, bool)>,
    pub suffix: Vec<(String, bool)>,
    pub bits: u32,
    pub min: f64,
    pub max: f64,
    pub coarsest: u64,
    pub finest: u64,
}

const DEFAULT_COARSEST: u64 = 0;
const DEFAULT_FINEST: u64 = 23;
const SHAPE_MAX_CELLS: usize = 8;
const SHAPE_MIN_LEVEL: u64 = 2;

impl GeoSpec {
    pub fn from_spec(spec: &Document) -> Option<GeoSpec> {
        let key = spec.get_document("key").ok()?;
        let mut prefix = Vec::new();
        let mut suffix = Vec::new();
        let mut field = None;
        let mut sphere = true;
        for (k, v) in key {
            match v {
                Bson::String(s) if s == "2dsphere" || s == "2d" => {
                    sphere = s == "2dsphere";
                    field = Some(k.clone());
                }
                other => {
                    let asc = !matches!(num(other), Some(n) if n < 0.0);
                    if field.is_some() {
                        suffix.push((k.clone(), asc));
                    } else {
                        prefix.push((k.clone(), asc));
                    }
                }
            }
        }
        Some(GeoSpec {
            sphere,
            field: field?,
            prefix,
            suffix,
            bits: spec.get("bits").and_then(num).map(|b| b as u32).unwrap_or(26),
            min: spec.get("min").and_then(num).unwrap_or(-180.0),
            max: spec.get("max").and_then(num).unwrap_or(180.0),
            coarsest: spec.get("coarsestIndexedLevel").and_then(num).map(|x| x as u64).unwrap_or(DEFAULT_COARSEST),
            finest: spec.get("finestIndexedLevel").and_then(num).map(|x| x as u64).unwrap_or(DEFAULT_FINEST),
        })
    }

    /// Morton code (geohash) of a planar point at the index's precision.
    pub fn hash2d(&self, x: f64, y: f64) -> u64 {
        let scale = (1u64 << self.bits) as f64;
        let cx = (((x - self.min) / (self.max - self.min)) * scale).floor().clamp(0.0, scale - 1.0) as u64;
        let cy = (((y - self.min) / (self.max - self.min)) * scale).floor().clamp(0.0, scale - 1.0) as u64;
        let mut z = 0u64;
        for i in 0..self.bits {
            z |= ((cx >> i) & 1) << (2 * i + 1);
            z |= ((cy >> i) & 1) << (2 * i);
        }
        z
    }
}

fn doc_text(d: &Document) -> String {
    crate::update::fmt_doc(d)
}

/// Cells of a document's geo field for a 2dsphere index: (cell tokens, whether any non-point shape was indexed).
pub fn sphere_keys(spec: &GeoSpec, d: &Document, vals: &[&Bson]) -> R<(Vec<u64>, bool)> {
    let mut cells: Vec<u64> = Vec::new();
    let mut coarse = false;
    for v in vals {
        match v {
            Bson::Null => continue,
            Bson::Array(a) if a.is_empty() => continue,
            Bson::Document(dd) if dd.is_empty() => continue,
            Bson::String(_) | Bson::Boolean(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
                return Err(MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  geo element must be an array or object: {}: {}", doc_text(d), spec.field, crate::update::fmt_bson(v))));
            }
            _ => {}
        }
        if matches!(v, Bson::Document(dd) if dd.contains_key("crs")) {
            return Err(MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  Custom CRS for big polygons is not supported in an index", doc_text(d))));
        }
        for e in element_list(v, d)? {
            match e {
                Elem::Legacy(p) => {
                    if p[0].abs() > 180.0 || p[1].abs() > 90.0 {
                        return Err(MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  can't project geometry into spherical CRS: [ {}, {} ]", doc_text(d), fmt_num(p[0]), fmt_num(p[1]))));
                    }
                    cells.push(CellID::from(&s2_point(&from_lnglat(p[0], p[1]))).0);
                }
                Elem::Geo(Shape::Point(p)) => cells.push(CellID::from(&s2_point(&p)).0),
                Elem::Geo(s) => {
                    coarse = true;
                    for part in flatten(&s) {
                        match part {
                            Shape::Point(p) => cells.push(CellID::from(&s2_point(&p)).0),
                            other => cells.extend(cover(&other, SHAPE_MAX_CELLS, SHAPE_MIN_LEVEL.max(spec.coarsest), spec.finest).into_iter().map(|c| c.0)),
                        }
                    }
                }
            }
        }
    }
    cells.sort_unstable();
    cells.dedup();
    Ok((cells, coarse))
}

fn flatten(s: &Shape) -> Vec<Shape> {
    match s {
        Shape::Collection(ss) => ss.iter().flat_map(flatten).collect(),
        Shape::MultiPoint(ps) => ps.iter().map(|p| Shape::Point(*p)).collect(),
        Shape::MultiLine(ls) => ls.iter().map(|l| Shape::Line(l.clone())).collect(),
        Shape::MultiPolygon(ps) => ps.iter().map(|p| Shape::Polygon(p.clone())).collect(),
        other => vec![other.clone()],
    }
}

/// Elements of a field value, with GeoJSON errors reported as the index would (code 16755).
fn element_list(v: &Bson, d: &Document) -> R<Vec<Elem>> {
    match v {
        Bson::Document(g) if g.contains_key("type") => {
            let s = parse_geojson(g).map_err(|e| MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  {e}", doc_text(d))))?;
            Ok(vec![Elem::Geo(s)])
        }
        Bson::Document(_) => legacy_pair(v).map(|p| vec![Elem::Legacy(p)]).ok_or_else(|| MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  geo element must be an array or object", doc_text(d)))),
        Bson::Array(a) => {
            if a.len() >= 2 && a.iter().take(2).all(|x| num(x).is_some()) {
                return Ok(vec![Elem::Legacy([num(&a[0]).unwrap(), num(&a[1]).unwrap()])]);
            }
            let mut out = Vec::new();
            for x in a {
                match x {
                    Bson::Array(_) => out.extend(element_list(x, d)?),
                    Bson::Document(_) => {
                        // an array of GeoJSON objects is read as one (invalid) coordinate pair
                        return Err(MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  Point must only contain numeric elements", doc_text(d))));
                    }
                    _ => return Err(MongoError::new(16755, "Location16755", format!("Can't extract geo keys: {}  Point must only contain numeric elements", doc_text(d)))),
                }
            }
            Ok(out)
        }
        _ => Ok(vec![]),
    }
}

/// Hash of a planar point for a 2d index (range-checked as MongoDB does).
pub fn plane_keys(spec: &GeoSpec, d: &Document, vals: &[&Bson]) -> R<Vec<u64>> {
    let mut out = Vec::new();
    let bad_format = || MongoError::new(16804, "Location16804", "location object expected, location array not in correct format".to_string());
    for v in vals {
        let pairs: Vec<[f64; 2]> = match v {
            Bson::Array(a) if a.is_empty() => vec![],
            Bson::Array(a) if matches!(a[0], Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)) => {
                if a.len() == 1 {
                    return Err(MongoError::new(13068, "Location13068", "geo field only has 1 element".to_string()));
                }
                match legacy_pair(&Bson::Array(a.clone())) {
                    Some(p) => vec![p],
                    None => return Err(bad_format()),
                }
            }
            Bson::Array(a) => {
                let mut ps = Vec::new();
                for x in a {
                    match x {
                        Bson::Array(_) | Bson::Document(_) => ps.push(legacy_pair(x).ok_or_else(bad_format)?),
                        _ => return Err(bad_format()),
                    }
                }
                ps
            }
            Bson::Document(dd) if dd.is_empty() => vec![],
            Bson::Document(_) => vec![legacy_pair(v).ok_or_else(bad_format)?],
            _ => vec![],
        };
        for p in pairs {
            let ok = |x: f64| x >= spec.min && x <= spec.max;
            if !ok(p[0]) || !ok(p[1]) {
                return Err(MongoError::new(13027, "Location13027", format!("point not in interval of [ {}, {} ] :: caused by :: {}", fmt_num(spec.min), fmt_num(spec.max), doc_text(d))));
            }
            out.push(spec.hash2d(p[0], p[1]));
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Scans
// ---------------------------------------------------------------------------------------------

/// Inclusive id ranges to read from a 2dsphere index to find every shape that may intersect the cells.
pub fn sphere_ranges(cells: &[CellID], coarsest: u64, with_ancestors: bool) -> Vec<(u64, u64)> {
    let mut r: Vec<(u64, u64)> = Vec::new();
    for c in cells {
        r.push((c.range_min().0, c.range_max().0));
        if with_ancestors {
            for lvl in coarsest..c.level() {
                let p = c.parent(lvl);
                r.push((p.0, p.0));
            }
        }
    }
    merge_ranges(r)
}

pub fn merge_ranges(mut r: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    r.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (lo, hi) in r {
        match out.last_mut() {
            Some(last) if lo <= last.1.saturating_add(1) => last.1 = last.1.max(hi),
            _ => out.push((lo, hi)),
        }
    }
    out
}

/// Covering shape of a query condition on a 2dsphere index, or `None` when the whole index must be read.
pub fn sphere_query_shape(c: &Cond) -> Option<Shape> {
    match c {
        Cond::Within(Region::Sphere(polys)) => Some(Shape::MultiPolygon(polys.clone())),
        Cond::Within(Region::CenterSphere(ctr, r)) => Some(Shape::Cap(*ctr, *r)),
        Cond::Within(Region::Planar(p)) => planar_bound_shape(p),
        Cond::Intersects(s) => Some(s.clone()),
        Cond::Near(n) => {
            if n.max.is_finite() {
                let rad = if n.meters { n.max / EARTH_RADIUS_M } else if n.spherical { n.max } else { n.max.to_radians() };
                Some(Shape::Cap(n.center, rad.min(std::f64::consts::PI)))
            } else {
                None
            }
        }
    }
}

/// A conservative spherical polygon for a planar region (its bounding box in longitude / latitude).
fn planar_bound_shape(p: &Planar) -> Option<Shape> {
    let (lo, hi) = planar_bbox(p);
    if lo[0] < -180.0 || hi[0] > 180.0 || lo[1] < -90.0 || hi[1] > 90.0 || hi[0] - lo[0] >= 179.0 || hi[1] - lo[1] >= 179.0 {
        return None;
    }
    // sample the box boundary densely so the great-circle edges stay close to the parallels
    let n = 16;
    let mut ring: Vec<V> = Vec::new();
    for i in 0..n {
        ring.push(from_lnglat(lo[0] + (hi[0] - lo[0]) * i as f64 / n as f64, lo[1]));
    }
    for i in 0..n {
        ring.push(from_lnglat(hi[0], lo[1] + (hi[1] - lo[1]) * i as f64 / n as f64));
    }
    for i in 0..n {
        ring.push(from_lnglat(hi[0] - (hi[0] - lo[0]) * i as f64 / n as f64, hi[1]));
    }
    for i in 0..n {
        ring.push(from_lnglat(lo[0], hi[1] - (hi[1] - lo[1]) * i as f64 / n as f64));
    }
    let mut ring = ring;
    ring.dedup_by(|a, b| angle(a, b) < 1e-12);
    if ring.len() < 3 {
        return None;
    }
    // widen by a hair so boundary points stay inside
    Some(Shape::Polygon(Poly::new(vec![Loop::new(ring, false)])))
}

pub fn planar_bbox(p: &Planar) -> ([f64; 2], [f64; 2]) {
    match p {
        Planar::Box(lo, hi) => (*lo, *hi),
        Planar::Center(c, r) => ([c[0] - r, c[1] - r], [c[0] + r, c[1] + r]),
        Planar::Polygon(pts) => {
            let mut lo = [f64::INFINITY; 2];
            let mut hi = [f64::NEG_INFINITY; 2];
            for p in pts {
                for i in 0..2 {
                    lo[i] = lo[i].min(p[i]);
                    hi[i] = hi[i].max(p[i]);
                }
            }
            (lo, hi)
        }
    }
}

/// Inclusive hash ranges to read from a 2d index for a planar bounding box (quadtree cells of the geohash).
pub fn plane_ranges(spec: &GeoSpec, lo: [f64; 2], hi: [f64; 2]) -> Vec<(u64, u64)> {
    let total = 2 * spec.bits;
    let width = spec.max - spec.min;
    let mut out: Vec<(u64, u64)> = Vec::new();
    // recursive subdivision of the square; a cell inside the box is emitted whole
    fn rec(spec: &GeoSpec, x0: f64, y0: f64, size: f64, level: u32, code: u64, lo: [f64; 2], hi: [f64; 2], budget: &mut i32, out: &mut Vec<(u64, u64)>, total: u32) {
        let (x1, y1) = (x0 + size, y0 + size);
        if x1 < lo[0] || x0 > hi[0] || y1 < lo[1] || y0 > hi[1] {
            return;
        }
        let inside = x0 >= lo[0] && x1 <= hi[0] && y0 >= lo[1] && y1 <= hi[1];
        let shift = total - 2 * level;
        if inside || level >= spec.bits || *budget <= 0 {
            let first = code << shift;
            let last = first | ((1u64 << shift) - 1);
            out.push((first, last));
            return;
        }
        *budget -= 3;
        let half = size / 2.0;
        // child codes interleave x (odd bit) and y (even bit): (xbit, ybit) -> 2*xbit + ybit
        rec(spec, x0, y0, half, level + 1, code << 2, lo, hi, budget, out, total);
        rec(spec, x0, y0 + half, half, level + 1, (code << 2) | 1, lo, hi, budget, out, total);
        rec(spec, x0 + half, y0, half, level + 1, (code << 2) | 2, lo, hi, budget, out, total);
        rec(spec, x0 + half, y0 + half, half, level + 1, (code << 2) | 3, lo, hi, budget, out, total);
    }
    let mut budget = 60;
    rec(spec, spec.min, spec.min, width, 0, 0, lo, hi, &mut budget, &mut out, total);
    merge_ranges(out)
}

/// Mapping of cond to bounding box for the planar (2d) index.
pub fn plane_query_bbox(c: &Cond) -> Option<([f64; 2], [f64; 2])> {
    match c {
        Cond::Within(Region::Planar(p)) => Some(planar_bbox(p)),
        Cond::Within(Region::CenterSphere(ctr, r)) => {
            let (lng, lat) = to_lnglat(ctr);
            let d = r.to_degrees();
            let lat_d = d;
            let lng_d = if lat.abs() + d >= 90.0 { 360.0 } else { d / lat.to_radians().cos().max(1e-9) };
            if lng - lng_d < -180.0 || lng + lng_d > 180.0 {
                return None; // wraps around the antimeridian: a planar index cannot bound it
            }
            Some(([lng - lng_d, lat - lat_d], [lng + lng_d, lat + lat_d]))
        }
        Cond::Within(Region::Sphere(polys)) => {
            let mut lo = [f64::INFINITY; 2];
            let mut hi = [f64::NEG_INFINITY; 2];
            for p in polys {
                for v in &p.loops[0].v {
                    let (x, y) = to_lnglat(v);
                    lo = [lo[0].min(x), lo[1].min(y)];
                    hi = [hi[0].max(x), hi[1].max(y)];
                }
            }
            Some((lo, hi))
        }
        Cond::Intersects(_) => None,
        Cond::Near(n) => {
            if n.max.is_finite() {
                let r = if n.spherical { n.max.to_degrees() } else { n.max };
                Some(([n.center_xy[0] - r, n.center_xy[1] - r], [n.center_xy[0] + r, n.center_xy[1] + r]))
            } else {
                None
            }
        }
    }
}

/// Index entries of every geo element cell, as a map keyed by the encoded token (for tests and diagnostics).
#[allow(dead_code)]
pub fn describe(spec: &GeoSpec, d: &Document, vals: &[&Bson]) -> R<BTreeMap<String, ()>> {
    let (cells, _) = sphere_keys(spec, d, vals)?;
    Ok(cells.into_iter().map(|c| (token(c), ())).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn poly(coords: Vec<(f64, f64)>) -> Poly {
        let v: Vec<V> = coords.iter().map(|(x, y)| from_lnglat(*x, *y)).collect();
        Poly::new(vec![Loop::new(v, false)])
    }

    #[test]
    fn square_contains() {
        let p = poly(vec![(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
        assert!(p.contains(&from_lnglat(1.0, 1.0)));
        assert!(!p.contains(&from_lnglat(3.0, 1.0)));
        assert!(!p.contains(&from_lnglat(-1.0, 1.0)));
        assert!(p.contains(&from_lnglat(0.0, 0.0)));
        let cw = poly(vec![(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0)]);
        assert!(cw.contains(&from_lnglat(1.0, 1.0)));
        assert!(!cw.contains(&from_lnglat(100.0, 1.0)));
    }

    #[test]
    fn match_points() {
        let d = doc! { "$geoWithin": { "$box": [[0, 0], [2, 2]] } };
        let p = Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]);
        assert!(match_field(&[&p], &d).unwrap());
        let q = doc! { "$near": { "$geometry": { "type": "Point", "coordinates": [0, 0] }, "$maxDistance": 200000 } };
        let near = Bson::Document(doc! { "type": "Point", "coordinates": [1, 0] });
        assert!(match_field(&[&near], &q).unwrap());
    }

    #[test]
    fn covering_contains_point_cell() {
        let sq = poly(vec![(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)]);
        let cells = cover(&Shape::Polygon(sq), 20, 0, 24);
        let leaf = CellID::from(&s2_point(&from_lnglat(1.0, 1.0)));
        assert!(cells.iter().any(|c| c.contains(&leaf)));
        let far = CellID::from(&s2_point(&from_lnglat(50.0, 1.0)));
        assert!(!cells.iter().any(|c| c.contains(&far)));
    }
}
