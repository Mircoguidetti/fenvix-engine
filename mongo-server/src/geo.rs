//! Geospatial values: GeoJSON and legacy coordinates, spherical geometry on the unit sphere (great-circle edges), and the
//! predicates behind `$geoWithin`, `$geoIntersects` and `$near`.
//!
//! Spherical distances are angles in radians; GeoJSON queries multiply by the earth radius MongoDB uses (6,378,100 m).
//! Polygons are normalized like MongoDB's S2 loops: unless the polygon carries the `strictwinding` CRS, every loop
//! encloses the smaller of its two sides.

use bson::{Bson, Document};
use s2::point::Point as S2Point;
use std::f64::consts::PI;

/// Hidden fields carrying `$geoNear` / `$near` results through a pipeline (`{$meta: "geoNearDistance"}`).
pub const DIST_KEY: &str = "\u{1}geoNearDistance";
pub const POINT_KEY: &str = "\u{1}geoNearPoint";

pub const EARTH_RADIUS_M: f64 = 6_378_100.0;

pub type V = [f64; 3];

pub fn dot(a: &V, b: &V) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

pub fn cross(a: &V, b: &V) -> V {
    [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]
}

pub fn norm(a: &V) -> f64 {
    dot(a, a).sqrt()
}

fn scale(a: &V, k: f64) -> V {
    [a[0] * k, a[1] * k, a[2] * k]
}

fn add(a: &V, b: &V) -> V {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

fn sub(a: &V, b: &V) -> V {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

pub fn unit(a: &V) -> V {
    let n = norm(a);
    if n == 0.0 {
        *a
    } else {
        scale(a, 1.0 / n)
    }
}

/// Point of a longitude / latitude in degrees (`S2LatLng::ToPoint`).
pub fn from_lnglat(lng: f64, lat: f64) -> V {
    let phi = lat.to_radians();
    let theta = lng.to_radians();
    let cosphi = phi.cos();
    [theta.cos() * cosphi, theta.sin() * cosphi, phi.sin()]
}

pub fn to_lnglat(v: &V) -> (f64, f64) {
    let lat = v[2].atan2((v[0] * v[0] + v[1] * v[1]).sqrt()).to_degrees();
    let lng = v[1].atan2(v[0]).to_degrees();
    (lng, lat)
}

/// Angle between two points (`S2Point::Angle`).
pub fn angle(a: &V, b: &V) -> f64 {
    norm(&cross(a, b)).atan2(dot(a, b))
}

pub fn s2_point(v: &V) -> S2Point {
    S2Point::from_coords(v[0], v[1], v[2])
}

// ---------------------------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Loop {
    /// Vertices without the closing duplicate.
    pub v: Vec<V>,
    /// The interior is the complement of the region to the left of the edges.
    flip: bool,
    /// Whether [`ORIGIN`] lies in the region to the left of the edges.
    origin_in: bool,
    /// Bounding cap: center and radius (radians).
    pub cap: (V, f64),
}

#[derive(Clone, Debug)]
pub struct Poly {
    /// `loops[0]` is the shell, the others are holes.
    pub loops: Vec<Loop>,
}

#[derive(Clone, Debug)]
pub enum Shape {
    Point(V),
    MultiPoint(Vec<V>),
    Line(Vec<V>),
    MultiLine(Vec<Vec<V>>),
    Polygon(Poly),
    MultiPolygon(Vec<Poly>),
    Collection(Vec<Shape>),
    /// Internal: a spherical cap (`$centerSphere`, search radii).
    Cap(V, f64),
}

/// Fixed reference point for crossing parity (`S2::Origin()`).
const ORIGIN: V = [-0.009_999_466_435_025_02, 0.002_592_454_260_932_412, 0.999_946_643_502_502];
fn origin() -> V {
    ORIGIN
}

/// Determinant sign by the stable formulation of the S2 library mongod embeds (`S2::ExpensiveCCW`): the longest
/// difference vector decides which vertex is the origin of the triple product.
fn det_stable(a: &V, b: &V, c: &V) -> f64 {
    let sab = if dot(a, b) > 0.0 { -1.0 } else { 1.0 };
    let sbc = if dot(b, c) > 0.0 { -1.0 } else { 1.0 };
    let sca = if dot(c, a) > 0.0 { -1.0 } else { 1.0 };
    let vab = add(a, &scale(b, sab));
    let vbc = add(b, &scale(c, sbc));
    let vca = add(c, &scale(a, sca));
    let n2 = |v: &V| dot(v, v);
    let (dab, dbc, dca) = (n2(&vab), n2(&vbc), n2(&vca));
    let lt = |p: &V, q: &V| p.partial_cmp(q) == Some(std::cmp::Ordering::Less);
    if dca < dbc || (dca == dbc && lt(a, b)) {
        if dab < dbc || (dab == dbc && lt(a, c)) {
            dot(&cross(&vab, &vca), a) * sab
        } else {
            dot(&cross(&vca, &vbc), c) * sca
        }
    } else if dab < dca || (dab == dca && lt(b, c)) {
        dot(&cross(&vbc, &vab), b) * sbc
    } else {
        dot(&cross(&vca, &vbc), c) * sca
    }
}

/// `S2::RobustCCW`: +1 counter-clockwise, -1 clockwise, 0 only when two points are equal. Numerically coplanar
/// triples are resolved by the library's own tie-break (sorted lexicographically, planar x-y, y-z, z-x), not by exact
/// arithmetic, so the answer for points exactly on an edge matches mongod rather than the geometry.
fn robust_ccw(a: &V, b: &V, c: &V) -> i32 {
    let det = dot(&cross(a, b), c);
    if det > 0.8e-15 {
        return 1;
    }
    if det < -0.8e-15 {
        return -1;
    }
    if a == b || b == c || c == a {
        return 0;
    }
    let s = det_stable(a, b, c);
    if s > 0.0 {
        return 1;
    }
    if s < 0.0 {
        return -1;
    }
    let mut p = [*a, *b, *c];
    let mut parity = 1;
    for i in 0..3 {
        for j in 0..2 - i {
            if p[j + 1].partial_cmp(&p[j]) == Some(std::cmp::Ordering::Less) {
                p.swap(j, j + 1);
                parity = -parity;
            }
        }
    }
    for (i, j) in [(0, 1), (1, 2), (2, 0)] {
        let d = (p[1][i] - p[0][i]) * (p[2][j] - p[0][j]) - (p[1][j] - p[0][j]) * (p[2][i] - p[0][i]);
        if d > 0.0 {
            return parity;
        }
        if d < 0.0 {
            return -parity;
        }
    }
    parity
}

/// Do the arcs `a-b` and `c-d` cross (not just touch at an endpoint)?
fn arcs_cross(a: &V, b: &V, c: &V, d: &V) -> bool {
    left_of(a, b, c) != left_of(a, b, d) && left_of(c, d, a) != left_of(c, d, b)
}

/// Orientation of three points: `true` when `c` is to the left of the directed great circle `a -> b`.
fn left_of(a: &V, b: &V, c: &V) -> bool {
    robust_ccw(a, b, c) > 0
}

/// `S2EdgeUtil::RobustCrossing`: 1 for a proper crossing, 0 when the edges share a vertex, -1 otherwise.
fn robust_crossing(a: &V, b: &V, c: &V, d: &V) -> i32 {
    if a == c || a == d || b == c || b == d {
        return 0;
    }
    let acb = -robust_ccw(a, b, c);
    let bda = robust_ccw(a, b, d);
    if acb == -bda && bda != 0 {
        return -1;
    }
    let cbd = -robust_ccw(c, d, b);
    if cbd != bda {
        return -1;
    }
    if robust_ccw(c, d, a) == bda && acb == bda {
        1
    } else {
        -1
    }
}

/// `S2::Ortho`
fn ortho(a: &V) -> V {
    let big = (0..3).fold(0, |m, i| if a[i].abs() > a[m].abs() { i } else { m });
    let k = (big + 2) % 3;
    let mut t = [0.012, 0.0053, 0.00457];
    t[k] = 1.0;
    unit(&cross(a, &t))
}

/// `S2::OrderedCCW`: are `a, b, c` in counter-clockwise order around `o`?
fn ordered(a: &V, b: &V, c: &V, o: &V) -> bool {
    let mut sum = 0;
    if robust_ccw(b, o, a) >= 0 {
        sum += 1;
    }
    if robust_ccw(c, o, b) >= 0 {
        sum += 1;
    }
    if robust_ccw(a, o, c) > 0 {
        sum += 1;
    }
    sum >= 2
}

/// `S2EdgeUtil::VertexCrossing`
fn vertex_crossing(a: &V, b: &V, c: &V, d: &V) -> bool {
    if a == b || c == d {
        return false;
    }
    if a == d {
        return ordered(&ortho(a), c, b, a);
    }
    if b == c {
        return ordered(&ortho(b), d, a, b);
    }
    if a == c {
        return ordered(&ortho(a), d, b, a);
    }
    if b == d {
        return ordered(&ortho(b), c, a, b);
    }
    false
}

/// `S2EdgeUtil::EdgeOrVertexCrossing`: proper crossings count, and shared vertices follow the vertex-crossing rule, so
/// that point-in-polygon parity is consistent (half-open boundaries).
fn xing(a: &V, b: &V, c: &V, d: &V) -> bool {
    match robust_crossing(a, b, c, d) {
        c if c < 0 => false,
        0 => vertex_crossing(a, b, c, d),
        _ => true,
    }
}

/// Do the edges cross properly (interior to interior)?
fn proper_cross(a: &V, b: &V, c: &V, d: &V) -> bool {
    robust_crossing(a, b, c, d) > 0
}

/// Do the edges cross or share a vertex (`S2Polyline::Intersects`)?
fn cross_or_touch(a: &V, b: &V, c: &V, d: &V) -> bool {
    robust_crossing(a, b, c, d) >= 0
}

/// Do the wedges `a0-ab1-a2` and `b0-ab1-b2` (interiors to the left) share interior points?
fn wedge_intersects(a0: &V, ab1: &V, a2: &V, b0: &V, b2: &V) -> bool {
    !(ordered(a0, b2, b0, ab1) && ordered(b0, a2, a0, ab1))
}

/// Does wedge A contain wedge B?
fn wedge_contains(a0: &V, ab1: &V, a2: &V, b0: &V, b2: &V) -> bool {
    ordered(a2, b2, b0, ab1) && ordered(b0, a0, a2, ab1)
}

/// Smallest angle from `p` to the arc `a-b`.
pub fn dist_to_arc(x: &V, a: &V, b: &V) -> f64 {
    // S2Polyline::Project / S2::GetClosestPoint: the closest point is the projection on the great circle when it lies
    // inside the arc, else the nearer endpoint; the distance is the angle to that point
    let n = cross(&add(b, a), &sub(b, a));
    let nn = dot(&n, &n);
    if nn > 0.0 {
        let p = sub(x, &scale(&n, dot(x, &n) / nn));
        if dot(&cross(&p, &n), a) > 0.0 && dot(&cross(&n, &p), b) > 0.0 {
            return angle(x, &unit(&p));
        }
    }
    let d2 = |q: &V| {
        let d = sub(x, q);
        dot(&d, &d)
    };
    if d2(a) <= d2(b) {
        angle(x, a)
    } else {
        angle(x, b)
    }
}

impl Loop {
    /// Interior to the left of the edges (`strict`), or the smaller side.
    pub fn new(v: Vec<V>, strict: bool) -> Loop {
        let mut l = Loop { v, flip: false, origin_in: false, cap: (origin(), 0.0) };
        l.cap = l.bound_cap();
        // origin: a point just left of the midpoint of edge 0 is inside the left region
        let (a, b) = (l.v[0], l.v[1 % l.v.len()]);
        let m = unit(&add(&a, &b));
        let left = unit(&cross(&a, &b));
        let eps = (angle(&a, &b) * 1e-6).max(1e-13);
        let probe = unit(&add(&m, &scale(&left, eps)));
        let parity = l.crossings(&origin(), &probe) % 2 == 1;
        l.origin_in = !parity;
        if !strict {
            l.flip = l.left_area() > 2.0 * PI;
        } else if l.left_area() > 2.0 * PI {
            l.cap.1 = PI; // the region is larger than a hemisphere: the vertex cap does not bound it
        }
        l
    }

    fn bound_cap(&self) -> (V, f64) {
        let mut c = [0.0; 3];
        for p in &self.v {
            c = add(&c, p);
        }
        let c = if norm(&c) < 1e-12 { self.v[0] } else { unit(&c) };
        let r = self.v.iter().map(|p| angle(&c, p)).fold(0.0, f64::max);
        (c, r)
    }

    /// Vertices ordered so that the interior is on the left of the edges.
    fn oriented(&self) -> Vec<V> {
        if self.flip {
            self.v.iter().rev().copied().collect()
        } else {
            self.v.clone()
        }
    }

    fn edges(&self) -> impl Iterator<Item = (&V, &V)> {
        let n = self.v.len();
        (0..n).map(move |i| (&self.v[i], &self.v[(i + 1) % n]))
    }

    fn crossings(&self, a: &V, b: &V) -> usize {
        self.edges().filter(|(c, d)| xing(a, b, c, d)).count()
    }

    /// Area (steradians) of the region to the left of the edges, in `(0, 4π)`.
    fn left_area(&self) -> f64 {
        let o = self.v[0];
        let mut s = 0.0;
        for i in 1..self.v.len() - 1 {
            let (b, c) = (self.v[i], self.v[i + 1]);
            s += 2.0 * dot(&o, &cross(&b, &c)).atan2(1.0 + dot(&o, &b) + dot(&b, &c) + dot(&c, &o));
        }
        if s < 0.0 {
            s + 4.0 * PI
        } else {
            s
        }
    }

    pub fn boundary_distance(&self, p: &V) -> f64 {
        self.edges().map(|(a, b)| dist_to_arc(p, a, b)).fold(f64::INFINITY, f64::min)
    }

    /// Strict interior test (boundary points answer arbitrarily; see [`Poly::contains`]).
    fn contains_interior(&self, p: &V) -> bool {
        let inside_left = self.origin_in ^ (self.crossings(&origin(), p) % 2 == 1);
        inside_left ^ self.flip
    }
}

const BOUNDARY_EPS: f64 = 1e-11;

impl Poly {
    pub fn new(loops: Vec<Loop>) -> Poly {
        Poly { loops }
    }

    /// Point in polygon, boundary included.
    pub fn contains(&self, p: &V) -> bool {
        let shell = &self.loops[0];
        if !shell.flip && angle(&shell.cap.0, p) > shell.cap.1 + 1e-9 {
            return false;
        }
        if self.loops.iter().any(|l| l.boundary_distance(p) < BOUNDARY_EPS) {
            return true;
        }
        if !shell.contains_interior(p) {
            return false;
        }
        !self.loops[1..].iter().any(|h| h.contains_interior(p))
    }

    /// mongod's `containsPoint`: S2's half-open `Contains`, else whether the point's leaf cell overlaps the polygon, so a
    /// point on an edge or vertex is inside when the cell on its side of the boundary reaches into the polygon.
    pub fn contains_point_s2(&self, p: &V) -> bool {
        if self.contains_halfopen(p) {
            return true;
        }
        let cell = s2::cell::Cell::from(&s2_point(p));
        let cv: Vec<V> = cell.vertices().iter().map(|q| [q.0.x, q.0.y, q.0.z]).collect();
        polys_intersect(self, &Poly::new(vec![Loop::new(cv, true)]))
    }

    /// Point in polygon with S2's half-open boundary (a boundary point belongs to exactly one of two adjacent polygons).
    fn contains_halfopen(&self, p: &V) -> bool {
        self.loops[0].contains_interior(p) && !self.loops[1..].iter().any(|h| h.contains_interior(p))
    }

    pub fn boundary_distance(&self, p: &V) -> f64 {
        self.loops.iter().map(|l| l.boundary_distance(p)).fold(f64::INFINITY, f64::min)
    }

    fn boundary_edges(&self) -> impl Iterator<Item = (&V, &V)> {
        self.loops.iter().flat_map(|l| l.edges())
    }
}

impl Shape {
    /// Smallest angle between `p` and the shape (0 inside a polygon or cap).
    pub fn dist_point(&self, p: &V) -> f64 {
        match self {
            Shape::Point(q) => angle(p, q),
            Shape::MultiPoint(qs) => qs.iter().map(|q| angle(p, q)).fold(f64::INFINITY, f64::min),
            Shape::Line(l) => line_dist(l, p),
            Shape::MultiLine(ls) => ls.iter().map(|l| line_dist(l, p)).fold(f64::INFINITY, f64::min),
            Shape::Polygon(pg) => poly_dist(pg, p),
            Shape::MultiPolygon(pgs) => pgs.iter().map(|pg| poly_dist(pg, p)).fold(f64::INFINITY, f64::min),
            Shape::Collection(ss) => ss.iter().map(|s| s.dist_point(p)).fold(f64::INFINITY, f64::min),
            Shape::Cap(c, r) => (angle(p, c) - r).max(0.0),
        }
    }

    /// Is the shape (or any part of a collection) made only of points?
    pub fn is_point_like(&self) -> bool {
        match self {
            Shape::Point(_) | Shape::MultiPoint(_) => true,
            Shape::Collection(ss) => ss.iter().all(|s| s.is_point_like()),
            _ => false,
        }
    }
}

fn line_dist(l: &[V], p: &V) -> f64 {
    if l.len() == 1 {
        return angle(p, &l[0]);
    }
    l.windows(2).map(|w| dist_to_arc(p, &w[0], &w[1])).fold(f64::INFINITY, f64::min)
}

fn poly_dist(pg: &Poly, p: &V) -> f64 {
    if pg.contains(p) {
        0.0
    } else {
        pg.boundary_distance(p)
    }
}

// ---------------------------------------------------------------------------------------------
// Predicates between shapes
// ---------------------------------------------------------------------------------------------

/// `S2Polyline::MayIntersect(S2Cell)` with the query point's leaf cell: mongod answers point-on-line this way, so a
/// point lying exactly on a stored line misses when the line runs along the leaf cell's edge.
fn line_may_intersect_cell(l: &[V], p: &V) -> bool {
    use s2::cell::Cell;
    let cell = Cell::from(&s2_point(p));
    if l.iter().any(|v| cell.contains_point(&s2_point(v))) {
        return true;
    }
    let cv = cell.vertices();
    let cv: Vec<V> = cv.iter().map(|q| [q.0.x, q.0.y, q.0.z]).collect();
    (0..4).any(|j| l.windows(2).any(|w| cross_or_touch(&cv[j], &cv[(j + 1) & 3], &w[0], &w[1])))
}

fn segs(l: &[V]) -> impl Iterator<Item = (&V, &V)> {
    l.windows(2).map(|w| (&w[0], &w[1]))
}

/// Do the interiors of two loops overlap (`S2Loop::Intersects`)? Boundary contact alone does not count.
fn loops_intersect(a: &Loop, b: &Loop) -> bool {
    let (av, bv) = (a.oriented(), b.oriented());
    let (na, nb) = (av.len(), bv.len());
    for i in 0..na {
        for j in 0..nb {
            if proper_cross(&av[i], &av[(i + 1) % na], &bv[j], &bv[(j + 1) % nb]) {
                return true;
            }
        }
    }
    for i in 0..na {
        for j in 0..nb {
            if av[i] == bv[j] && wedge_intersects(&av[(i + na - 1) % na], &av[i], &av[(i + 1) % na], &bv[(j + nb - 1) % nb], &bv[(j + 1) % nb]) {
                return true;
            }
        }
    }
    if let Some(p) = bv.iter().find(|p| !av.contains(p)) {
        if a.contains_interior(p) {
            return true;
        }
    }
    if let Some(p) = av.iter().find(|p| !bv.contains(p)) {
        if b.contains_interior(p) {
            return true;
        }
    }
    false
}


/// Does loop `a` contain loop `b` (`S2Loop::Contains`)?
fn loop_contains(a: &Loop, b: &Loop) -> bool {
    let (av, bv) = (a.oriented(), b.oriented());
    let (na, nb) = (av.len(), bv.len());
    for i in 0..na {
        for j in 0..nb {
            if proper_cross(&av[i], &av[(i + 1) % na], &bv[j], &bv[(j + 1) % nb]) {
                return false;
            }
        }
    }
    for i in 0..na {
        for j in 0..nb {
            if av[i] == bv[j] && !wedge_contains(&av[(i + na - 1) % na], &av[i], &av[(i + 1) % na], &bv[(j + nb - 1) % nb], &bv[(j + 1) % nb]) {
                return false;
            }
        }
    }
    match bv.iter().find(|p| !av.contains(p)) {
        Some(p) => a.contains_interior(p),
        None => true,
    }
}

fn polys_intersect(x: &Poly, y: &Poly) -> bool {
    if !loops_intersect(&x.loops[0], &y.loops[0]) {
        return false;
    }
    // one shell lying entirely inside a hole of the other leaves them disjoint
    let in_hole = |shell: &Loop, other: &Poly| other.loops[1..].iter().any(|h| loop_contains(h, shell));
    !(in_hole(&y.loops[0], x) || in_hole(&x.loops[0], y))
}

fn poly_contains_poly(a: &Poly, b: &Poly) -> bool {
    if !loop_contains(&a.loops[0], &b.loops[0]) {
        return false;
    }
    // every hole of A that overlaps B must be covered by a hole of B
    a.loops[1..].iter().all(|h| !loops_intersect(&b.loops[0], h) || b.loops[1..].iter().any(|bh| loop_contains(bh, h)))
}

/// Parameters (0..=1) along the edge `a-b` where it crosses the polygon boundary (shared vertices at 0 and 1).
fn edge_crossings_with(a: &V, b: &V, pg: &Poly) -> Vec<f64> {
    let mut ts: Vec<f64> = Vec::new();
    let ab = angle(a, b);
    for (c, d) in pg.boundary_edges() {
        if !xing(a, b, c, d) {
            continue;
        }
        let t = if a == c || a == d {
            0.0
        } else if b == c || b == d {
            1.0
        } else {
            let n = cross(&cross(a, b), &cross(c, d));
            let x = unit(&n);
            let x = if dot(&x, &add(a, b)) < 0.0 { scale(&x, -1.0) } else { x };
            if ab > 0.0 { (angle(a, &x) / ab).clamp(0.0, 1.0) } else { 0.0 }
        };
        ts.push(t);
    }
    ts.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
    ts
}

/// The parts of the edge `a-b` inside the polygon (`S2Polygon::IntersectWithPolyline` for one edge), as `(t0, t1)` intervals.
fn edge_inside_parts(a: &V, b: &V, pg: &Poly) -> Vec<(f64, f64)> {
    let mut inside = pg.contains_halfopen(a);
    let mut prev = 0.0;
    let mut out = Vec::new();
    for t in edge_crossings_with(a, b, pg) {
        if inside && t > prev {
            out.push((prev, t));
        }
        inside = !inside;
        prev = t;
    }
    if inside && prev < 1.0 {
        out.push((prev, 1.0));
    }
    out
}

fn line_poly_intersects(l: &[V], pg: &Poly) -> bool {
    if l.len() < 2 {
        return l.first().map(|p| pg.contains(p)).unwrap_or(false);
    }
    segs(l).any(|(a, b)| !edge_inside_parts(a, b, pg).is_empty())
}

fn line_in_poly(l: &[V], pg: &Poly) -> bool {
    segs(l).all(|(a, b)| {
        let parts = edge_inside_parts(a, b, pg);
        parts.len() == 1 && parts[0].0 <= 0.0 && parts[0].1 >= 1.0
    })
}

/// Does the shape `a` lie inside the polygon region `r` (a multi-polygon), boundary included for points?
pub fn within(a: &Shape, r: &[Poly]) -> bool {
    match a {
        Shape::Point(p) => r.iter().any(|pg| pg.contains_point_s2(p)),
        Shape::MultiPoint(ps) => ps.iter().all(|p| r.iter().any(|pg| pg.contains_point_s2(p))),
        Shape::Line(l) => r.iter().any(|pg| line_in_poly(l, pg)),
        Shape::MultiLine(ls) => ls.iter().all(|l| r.iter().any(|pg| line_in_poly(l, pg))),
        Shape::Polygon(pg) => r.iter().any(|q| poly_contains_poly(q, pg)),
        Shape::MultiPolygon(pgs) => pgs.iter().all(|pg| r.iter().any(|q| poly_contains_poly(q, pg))),
        Shape::Collection(ss) => ss.iter().all(|s| within(s, r)),
        Shape::Cap(c, rad) => r.iter().any(|pg| pg.contains(c) && pg.boundary_distance(c) >= *rad),
    }
}

/// Do two shapes share at least one point (S2 semantics for shared boundaries)?
pub fn intersects(a: &Shape, b: &Shape) -> bool {
    use Shape::*;
    match (a, b) {
        (Collection(xs), _) => xs.iter().any(|x| intersects(x, b)),
        (_, Collection(ys)) => ys.iter().any(|y| intersects(a, y)),
        (MultiPoint(xs), _) => xs.iter().any(|x| intersects(&Point(*x), b)),
        (_, MultiPoint(ys)) => ys.iter().any(|y| intersects(a, &Point(*y))),
        (MultiLine(xs), _) => xs.iter().any(|x| intersects(&Line(x.clone()), b)),
        (_, MultiLine(ys)) => ys.iter().any(|y| intersects(a, &Line(y.clone()))),
        (MultiPolygon(xs), _) => xs.iter().any(|x| intersects(&Polygon(x.clone()), b)),
        (_, MultiPolygon(ys)) => ys.iter().any(|y| intersects(a, &Polygon(y.clone()))),
        (Cap(c, r), other) | (other, Cap(c, r)) => other.dist_point(c) <= *r,
        (Point(p), Point(q)) => angle(p, q) < BOUNDARY_EPS,
        (Point(p), Line(l)) | (Line(l), Point(p)) => line_may_intersect_cell(l, p),
        (Point(p), Polygon(pg)) | (Polygon(pg), Point(p)) => pg.contains_point_s2(p),
        (Line(x), Line(y)) => segs(x).any(|(p, q)| segs(y).any(|(r, s)| cross_or_touch(p, q, r, s))),
        (Line(l), Polygon(pg)) | (Polygon(pg), Line(l)) => line_poly_intersects(l, pg),
        (Polygon(x), Polygon(y)) => polys_intersect(x, y),
    }
}

// ---------------------------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------------------------

type PR<T> = Result<T, String>;

pub fn fmt_num(x: f64) -> String {
    if x == x.trunc() && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        format!("{x}")
    }
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

fn point_of(b: &Bson) -> PR<V> {
    let Bson::Array(a) = b else { return Err("Point must be an array".to_string()) };
    if a.len() < 2 || a.iter().any(|x| num(x).is_none()) {
        return Err("Point must only contain numeric elements".to_string());
    }
    lnglat_checked(num(&a[0]).unwrap(), num(&a[1]).unwrap())
}

pub fn lnglat_checked(lng: f64, lat: f64) -> PR<V> {
    if !(lng.abs() <= 180.0 && lat.abs() <= 90.0) {
        return Err(format!("Longitude/latitude is out of bounds, lng: {} lat: {}", fmt_num(lng), fmt_num(lat)));
    }
    Ok(from_lnglat(lng, lat))
}

fn line_of(b: &Bson) -> PR<Vec<V>> {
    let Bson::Array(a) = b else { return Err("LineString coordinates must be an array".to_string()) };
    let pts: Vec<V> = a.iter().map(point_of).collect::<PR<_>>()?;
    if pts.len() < 2 {
        return Err(format!("GeoJSON LineString must have at least 2 vertices: {}", crate::update::fmt_bson(b)));
    }
    for w in pts.windows(2) {
        if angle(&w[0], &w[1]) == 0.0 {
            return Err(format!("Duplicate vertices in LineString: {}", crate::update::fmt_bson(b)));
        }
    }
    Ok(pts)
}

fn loop_of(b: &Bson, strict: bool) -> PR<Loop> {
    let Bson::Array(a) = b else { return Err("Polygon ring must be an array".to_string()) };
    let mut pts: Vec<V> = a.iter().map(point_of).collect::<PR<_>>()?;
    let text = crate::update::fmt_bson(b);
    if pts.first() != pts.last() {
        return Err(format!("Loop is not closed, first vertex does not equal last vertex: {text}"));
    }
    pts.pop();
    pts.dedup();
    if pts.len() > 1 && pts.first() == pts.last() {
        pts.pop();
    }
    let mut distinct: Vec<V> = Vec::new();
    for p in &pts {
        if !distinct.iter().any(|q| angle(p, q) == 0.0) {
            distinct.push(*p);
        }
    }
    if distinct.len() < 3 {
        return Err(format!("Loop must have at least 3 different vertices, {} unique vertices were provided: {text}", distinct.len()));
    }
    let n = pts.len();
    for i in 0..n {
        if angle(&pts[i], &pts[(i + 1) % n]) == 0.0 {
            return Err(format!("Loop is not valid: {text} Duplicate vertices: {i} and {}", (i + 1) % n));
        }
    }
    let l = Loop::new(pts, strict);
    let edges: Vec<(V, V)> = l.edges().map(|(a, b)| (*a, *b)).collect();
    // self-intersection of non-adjacent edges, with a cheap bounding-cap rejection
    for i in 0..n {
        for j in i + 2..n {
            if i == 0 && j == n - 1 {
                continue;
            }
            if arcs_cross(&edges[i].0, &edges[i].1, &edges[j].0, &edges[j].1) {
                let (a, b) = (to_lnglat(&edges[i].0), to_lnglat(&edges[i].1));
                let (c, d) = (to_lnglat(&edges[j].0), to_lnglat(&edges[j].1));
                return Err(format!(
                    "Loop is not valid: {text} Edges {i} and {j} cross. Edge locations in degrees: [{:.7}, {:.7}]-[{:.7}, {:.7}] and [{:.7}, {:.7}]-[{:.7}, {:.7}]",
                    a.0, a.1, b.0, b.1, c.0, c.1, d.0, d.1
                ));
            }
        }
    }
    Ok(l)
}

fn poly_of(b: &Bson, strict: bool) -> PR<Poly> {
    let Bson::Array(rings) = b else { return Err("Polygon coordinates must be an array".to_string()) };
    if rings.is_empty() {
        return Err("Polygon coordinates must contain at least one linear ring".to_string());
    }
    let loops: Vec<Loop> = rings.iter().map(|r| loop_of(r, strict)).collect::<PR<_>>()?;
    let pg = Poly::new(loops);
    for (i, h) in pg.loops.iter().enumerate().skip(1) {
        let shell = &pg.loops[0];
        if !h.v.iter().all(|p| shell.boundary_distance(p) < BOUNDARY_EPS || shell.contains_interior(p)) {
            return Err(format!(
                "Secondary loops not contained by first exterior loop - secondary loops must be holes: {} first loop: {}",
                crate::update::fmt_bson(&rings[i]),
                crate::update::fmt_bson(&rings[0])
            ));
        }
    }
    Ok(pg)
}

/// A GeoJSON object (`{type, coordinates | geometries, crs?}`).
pub fn parse_geojson(d: &Document) -> PR<Shape> {
    let ty = match d.get("type") {
        Some(Bson::String(s)) => s.as_str(),
        _ => return Err(format!("unknown GeoJSON type: {}", crate::update::fmt_doc(d))),
    };
    let mut strict = false;
    if let Some(crs) = d.get("crs") {
        const STRICT: &str = "urn:x-mongodb:crs:strictwinding:EPSG:4326";
        let name = match crs {
            Bson::Document(c) if c.get_str("type").ok() == Some("name") => c.get_document("properties").ok().and_then(|p| p.get_str("name").ok()),
            _ => return Err(format!("CRS must have field \"type\": \"name\": {}", crate::update::fmt_doc(d))),
        };
        match name {
            Some(STRICT) => strict = true,
            Some(other) => return Err(format!("Unknown CRS name: {other}")),
            None => return Err(format!("CRS must have field \"properties.name\": {}", crate::update::fmt_doc(d))),
        }
        if !matches!(ty, "Polygon" | "MultiPolygon") {
            return Err(format!("Only Polygon and MultiPolygon support a CRS: {}", crate::update::fmt_doc(d)));
        }
    }
    let coords = || d.get("coordinates").ok_or_else(|| format!("GeoJSON {ty} must have a coordinates array: {}", crate::update::fmt_doc(d)));
    let arr = |b: &Bson| -> PR<Vec<Bson>> {
        match b {
            Bson::Array(a) => Ok(a.clone()),
            _ => Err(format!("{ty} coordinates must be an array")),
        }
    };
    match ty {
        "Point" => Ok(Shape::Point(point_of(coords()?)?)),
        "MultiPoint" => {
            let pts: Vec<V> = arr(coords()?)?.iter().map(point_of).collect::<PR<_>>()?;
            if pts.is_empty() {
                return Err("MultiPoint coordinates must have at least 1 element".to_string());
            }
            Ok(Shape::MultiPoint(pts))
        }
        "LineString" => Ok(Shape::Line(line_of(coords()?)?)),
        "MultiLineString" => {
            let ls: Vec<Vec<V>> = arr(coords()?)?.iter().map(line_of).collect::<PR<_>>()?;
            if ls.is_empty() {
                return Err("MultiLineString coordinates must have at least 1 element".to_string());
            }
            Ok(Shape::MultiLine(ls))
        }
        "Polygon" => Ok(Shape::Polygon(poly_of(coords()?, strict)?)),
        "MultiPolygon" => {
            let ps: Vec<Poly> = arr(coords()?)?.iter().map(|p| poly_of(p, strict)).collect::<PR<_>>()?;
            if ps.is_empty() {
                return Err("MultiPolygon coordinates must have at least 1 element".to_string());
            }
            Ok(Shape::MultiPolygon(ps))
        }
        "GeometryCollection" => {
            let gs = match d.get("geometries") {
                Some(Bson::Array(g)) => g,
                _ => return Err(format!("GeometryCollection must have a geometries array: {}", crate::update::fmt_doc(d))),
            };
            if gs.is_empty() {
                return Err("GeometryCollection geometries must have at least 1 element".to_string());
            }
            let mut out = Vec::new();
            for g in gs {
                match g {
                    Bson::Document(gd) => out.push(parse_geojson(gd)?),
                    _ => return Err("GeometryCollection geometries must be objects".to_string()),
                }
            }
            Ok(Shape::Collection(out))
        }
        _ => Err(format!("unknown GeoJSON type: {}", crate::update::fmt_doc(d))),
    }
}

/// Legacy coordinate pair: `[x, y]` or an embedded document with two numeric fields.
pub fn legacy_pair(b: &Bson) -> Option<[f64; 2]> {
    match b {
        Bson::Array(a) if a.len() >= 2 => Some([num(&a[0])?, num(&a[1])?]),
        Bson::Document(d) if !d.contains_key("type") && d.len() >= 2 => {
            let mut it = d.values();
            Some([num(it.next()?)?, num(it.next()?)?])
        }
        _ => None,
    }
}

/// How a document value takes part in geo queries: one or more elements, each a GeoJSON shape or a legacy point.
#[derive(Clone, Debug)]
pub enum Elem {
    Geo(Shape),
    Legacy([f64; 2]),
}

/// The geo elements of a field value: an array of pairs is several legacy points, `[x, y]` one point, GeoJSON one
/// shape. Values that are not geometries yield nothing.
pub fn elements(v: &Bson) -> Vec<Elem> {
    match v {
        // a stored geometry with a CRS (a "big" polygon) is not a usable geometry
        Bson::Document(d) if d.contains_key("type") && d.contains_key("crs") => vec![],
        Bson::Document(d) if d.contains_key("type") => parse_geojson(d).map(|s| vec![Elem::Geo(s)]).unwrap_or_default(),
        Bson::Document(_) => legacy_pair(v).map(|p| vec![Elem::Legacy(p)]).unwrap_or_default(),
        Bson::Array(a) => {
            if a.len() >= 2 && a.iter().take(2).all(|x| num(x).is_some()) {
                return vec![Elem::Legacy([num(&a[0]).unwrap(), num(&a[1]).unwrap()])];
            }
            a.iter().flat_map(elements).collect()
        }
        _ => vec![],
    }
}

/// Like [`elements`], with the document's own value for each element.
pub fn elements_raw(v: &Bson) -> Vec<(Elem, Bson)> {
    match v {
        Bson::Document(_) => elements(v).into_iter().map(|e| (e, v.clone())).collect(),
        Bson::Array(a) => {
            if a.len() >= 2 && a.iter().take(2).all(|x| num(x).is_some()) {
                return elements(v).into_iter().map(|e| (e, v.clone())).collect();
            }
            a.iter().flat_map(elements_raw).collect()
        }
        _ => vec![],
    }
}

/// The sphere point of an element, when it can be treated as one.
pub fn elem_point(e: &Elem) -> Option<V> {
    match e {
        Elem::Legacy(p) => {
            if p[0].abs() <= 180.0 && p[1].abs() <= 90.0 {
                Some(from_lnglat(p[0], p[1]))
            } else {
                None
            }
        }
        Elem::Geo(Shape::Point(p)) => Some(*p),
        _ => None,
    }
}

pub fn elem_shape(e: &Elem) -> Option<Shape> {
    match e {
        Elem::Geo(s) => Some(s.clone()),
        Elem::Legacy(_) => elem_point(e).map(Shape::Point),
    }
}

/// GeoJSON of a point for `geoNearPoint` / `includeLocs`.
pub fn point_doc(v: &V) -> Bson {
    let (lng, lat) = to_lnglat(v);
    Bson::Document(bson::doc! { "type": "Point", "coordinates": [lng, lat] })
}
