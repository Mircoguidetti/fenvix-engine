//! Deterministic hand-written scenarios.
use journal3z_core::*;
use std::collections::BTreeMap;

fn b(x: u64) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(&x.to_le_bytes())
}

/// walbench cycle 1, replayed on the journal3z protocol: a writer acknowledges writes with node 0 down (nodes 1+2),
/// then recovery starts while node 0 is still down. The walbench reader (fixed pair 0,2) cannot connect to node 0 and
/// fails; the journal3z open fences and reads ANY 2 of 3 and recovers every acknowledged write.
fn walbench_cycle1() {
    let mut nodes: BTreeMap<NodeId, NodeCore> = (0..3).map(|i| (i, NodeCore::new(i, Mutations::default()))).collect();
    let deliver = |nodes: &mut BTreeMap<NodeId, NodeCore>, w: &mut WriterDb, msgs: Vec<(NodeId, Msg)>, down: &[NodeId]| -> Vec<WEvent> {
        let mut q = msgs;
        let mut ev = vec![];
        while !q.is_empty() {
            let mut next = vec![];
            for (n, m) in q {
                if down.contains(&n) {
                    continue; // connection refused
                }
                for (_, r) in nodes.get_mut(&n).unwrap().handle(1000, m).replies {
                    let o = w.on_msg(n, r);
                    next.extend(o.msgs);
                    ev.extend(o.events);
                }
            }
            q = next;
        }
        ev
    };
    let down = [0];
    let (mut a, o) = WriterDb::open(1, 1, vec![0, 1, 2], 0, Mutations::default());
    deliver(&mut nodes, &mut a, o.msgs, &down);
    let mut acked = vec![];
    for id in 1..=200u64 {
        let i = a.append(b(id)).unwrap();
        let mut o = WOut::default();
        a.flush(&mut o);
        if deliver(&mut nodes, &mut a, o.msgs, &down).iter().any(|e| matches!(e, WEvent::Committed(c) if *c >= i)) {
            acked.push((i, id));
        }
    }
    // kill -9 of the writer; recovery while node 0 is still restarting
    let fixed_pair = [0u32, 2];
    let walbench = if fixed_pair.iter().any(|n| down.contains(n)) { "recover failed: node 0: connection refused (fixed pair 0,2)" } else { "ok" };
    let (mut bw, o) = WriterDb::open(1, 2, vec![0, 1, 2], 0, Mutations::default());
    let ev = deliver(&mut nodes, &mut bw, o.msgs, &down);
    let replay = ev.iter().find_map(|e| if let WEvent::Active { replay, .. } = e { Some(replay.clone()) } else { None });
    let rec = replay.map(|r| r.iter().map(|e| (e.idx, u64::from_le_bytes(e.data[..8].try_into().unwrap()))).collect::<Vec<_>>()).unwrap_or_default();
    let lost = acked.iter().filter(|x| !rec.contains(x)).count();
    println!(
        "{}",
        serde_json::json!({"scenario": "walbench_cycle1", "acknowledged_with_node0_down": acked.len(), "walbench_fixed_pair_reader": walbench,
            "journal3z_open_any_2_of_3": if rec.is_empty() { "failed" } else { "recovered" }, "recovered": rec.len(), "lost": lost})
    );
    assert_eq!(lost, 0);
    assert_eq!(acked.len(), 200);
}

pub fn run(name: &str) {
    match name {
        "walbench_cycle1" => walbench_cycle1(),
        "forget_fence" => {
            for m in [Mutations::default(), Mutations::parse("forget_fence").unwrap()] {
                println!("{}", forget_fence_loss(m));
            }
        }
        x => panic!("unknown scenario {x}"),
    }
}

/// The harm done by forgetting a fence (negative control `forget_fence`), deterministically: writer A (epoch 1) is
/// frozen; B (epoch 2) takes over, writes, uploads and trims, so the nodes' logs become empty; A resumes and appends.
/// With the fence record kept, A is rejected. Without it, A gets 2 acks for an index that S3 already holds with B's
/// write: an acknowledged write that exists nowhere.
fn forget_fence_loss(m: Mutations) -> serde_json::Value {
    let mut nodes: BTreeMap<NodeId, NodeCore> = (1..=3).map(|i| (i, NodeCore::new(i, m))).collect();
    fn pump(nodes: &mut BTreeMap<NodeId, NodeCore>, w: &mut WriterDb, msgs: Vec<(NodeId, Msg)>, down: &[NodeId]) -> Vec<WEvent> {
        let mut q = msgs;
        let mut ev = vec![];
        while !q.is_empty() {
            let mut next = vec![];
            for (n, msg) in q {
                if down.contains(&n) {
                    continue;
                }
                for (_, r) in nodes.get_mut(&n).unwrap().handle(1000, msg).replies {
                    let o = w.on_msg(n, r);
                    next.extend(o.msgs);
                    ev.extend(o.events);
                }
            }
            q = next;
        }
        ev
    }
    fn write(nodes: &mut BTreeMap<NodeId, NodeCore>, w: &mut WriterDb, id: u64, down: &[NodeId]) -> (Idx, bool) {
        let i = w.append(b(id)).unwrap();
        let mut o = WOut::default();
        w.flush(&mut o);
        let ev = pump(nodes, w, o.msgs, down);
        (i, ev.iter().any(|e| matches!(e, WEvent::Committed(c) if *c >= i)))
    }
    let mut s3: BTreeMap<Idx, u64> = BTreeMap::new();
    let (mut a, o) = WriterDb::open(9, 1, vec![1, 2, 3], 0, m);
    pump(&mut nodes, &mut a, o.msgs, &[]);
    for id in 1..=4 {
        let (i, _) = write(&mut nodes, &mut a, id, &[]);
        s3.insert(i, id);
    }
    let k = a.last;
    let o = a.uploaded(k);
    pump(&mut nodes, &mut a, o.msgs, &[]);
    // A freezes. B takes over (node 3 unreachable for B), writes, uploads, trims: logs on nodes 1 and 2 become empty.
    let (mut bw, o) = WriterDb::open(9, 2, vec![1, 2, 3], k, m);
    pump(&mut nodes, &mut bw, o.msgs, &[3]);
    for id in 100..=102 {
        let (i, _) = write(&mut nodes, &mut bw, id, &[3]);
        s3.insert(i, id);
    }
    let kb = bw.last;
    let o = bw.uploaded(kb);
    pump(&mut nodes, &mut bw, o.msgs, &[3]);
    // A resumes (its clock says its lease is still valid) and appends a client write
    let (i, acked) = write(&mut nodes, &mut a, 777, &[]);
    let in_s3 = s3.get(&i) == Some(&777);
    serde_json::json!({"forget_fence": m.forget_fence, "stale_writer_write_index": i, "acknowledged": acked, "s3_marker": kb,
        "s3_holds_it": in_s3, "lost_acknowledged_write": acked && !in_s3 && i <= kb, "stale_writer_told_stale": matches!(a.phase, Phase::Stale(_))})
}
