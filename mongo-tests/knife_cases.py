"""Geometry knife-edge battery: shapes sharing polygon edges / vertices, lines along edges, points on edges, repeated at
several offsets and scales (different S2 faces and rounding patterns). Used by geo_knife_battery.py and gen_tests_round10.py."""

OFFS = [(0,0,1),(0,0,0.01),(88,0,1),(-92,0,1),(172,0,1),(0,60,1),(0,-70,1),(45,45,1),(135,-35,1),(-135,35,1),(10,80,0.5),(0,0,10),(30,0,3)]
def build(ox,oy,s):
    T = lambda x,y: [round(ox+x*s,12), round(oy+y*s,12)]
    P = lambda x,y: {"type":"Point","coordinates":T(x,y)}
    L = lambda *p: {"type":"LineString","coordinates":[T(*x) for x in p]}
    def poly(*pts): return {"type":"Polygon","coordinates":[[T(*p) for p in pts]+[T(*pts[0])]]}
    Q = poly((2,0),(4,0),(4,2),(2,2))
    stored = {
     "poly_shared_edge": poly((0,0),(2,0),(2,2),(0,2)),
     "poly_shared_partial_edge": poly((0,0.5),(2,0.5),(2,1.5),(0,1.5)),
     "poly_shared_edge_east": poly((4,0),(6,0),(6,2),(4,2)),
     "poly_shared_edge_north": poly((2,2),(4,2),(4,4),(2,4)),
     "poly_shared_edge_south": poly((2,-2),(4,-2),(4,0),(2,0)),
     "poly_vertex_touch": poly((0,2),(2,2),(2,4),(0,4)),
     "poly_corner_only": poly((1,2),(2,2),(1.5,3)),
     "poly_identical": Q,
     "poly_inside_sharing_edge": poly((2,0),(3,0),(3,1),(2,1)),
     "poly_inside_sharing_east": poly((3,0.5),(4,0.5),(4,1.5),(3,1.5)),
     "poly_inside_sharing_north": poly((2.5,1),(3.5,1),(3.5,2),(2.5,2)),
     "poly_inside_sharing_south": poly((2.5,0),(3.5,0),(3.5,1),(2.5,1)),
     "poly_inside": poly((2.5,0.5),(3.5,0.5),(3.5,1.5),(2.5,1.5)),
     "poly_contains_q": poly((1,-1),(5,-1),(5,3),(1,3)),
     "poly_contains_sharing_edge": poly((2,-1),(5,-1),(5,3),(2,3)),
     "poly_overlap": poly((3,1),(5,1),(5,3),(3,3)),
     "line_touch_vertex_outside": L((0,0),(2,2)),
     "line_touch_edge_from_outside": L((1,1),(2,1)),
     "line_ends_inside": L((1,1),(3,1)),
     "line_along_west_full": L((2,0),(2,2)),
     "line_along_west_partial": L((2,0.5),(2,1.5)),
     "line_along_west_extends": L((2,-1),(2,1)),
     "line_along_west_extends_both": L((2,-1),(2,3)),
     "line_along_south_full": L((2,0),(4,0)),
     "line_along_south_partial": L((2.5,0),(3.5,0)),
     "line_along_east_partial": L((4,0.5),(4,1.5)),
     "line_along_north_partial": L((2.5,2),(3.5,2)),
     "line_along_north_full": L((2,2),(4,2)),
     "line_two_edges": L((2,0),(2,2),(4,2)),
     "line_inside": L((2.5,0.5),(3.5,1.5)),
     "line_crossing": L((1,1),(5,1)),
     "line_collinear_outside": L((2,-1),(2,-0.5)),
     "line_touch_vertex_inside_out": L((2,2),(1,3)),
     "point_vertex": P(2,2), "point_vertex2": P(4,0), "point_edge_w": P(2,1), "point_edge_e": P(4,1),
     "point_edge_n": P(3,2), "point_edge_s": P(3,0), "point_inside": P(3,1), "point_outside": P(1,1),
     "multipoint_vertex": {"type":"MultiPoint","coordinates":[T(2,2),T(9,9)]},
    }
    queries = {"Q":Q,
      "lineW":L((2,0),(2,2)),"lineW_ext":L((2,-1),(2,1)),"lineW_part":L((2,0.5),(2,1.5)),
      "lineS":L((2,0),(4,0)),"lineN_part":L((2.5,2),(3.5,2)),"lineE":L((4,0),(4,2)),
      "lineV":L((0,0),(2,2)),"lineIn":L((2.5,0.5),(3.5,1.5)),
      "polyL":poly((0,0),(2,0),(2,2),(0,2)),"polyIn":poly((2,0),(3,0),(3,1),(2,1)),
      "polyBig":poly((2,-1),(5,-1),(5,3),(2,3)),
      "ptV":P(2,2),"ptEw":P(2,1),"ptEs":P(3,0),"ptEn":P(3,2),"ptEe":P(4,1)}
    return stored, queries
