use bytes::Bytes;
use journal3z_api::local::{Config, LocalJournal};
use journal3z_api::{DbKey, Epoch, Index, Journal, JournalError};
use std::io::Write;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

#[tokio::test]
async fn append_durable_replay_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let a = DbKey::new("a", 1);
    let j = LocalJournal::open_dir(dir.path(), Config::default()).unwrap();
    let o = j.open(&a, Epoch(1), Index(0)).await.unwrap();
    assert_eq!(o.next, Index(1));
    assert!(o.replay.is_empty());
    let mut idx = vec![];
    for i in 0..10 {
        let ap = j.append(&a, Epoch(1), b(&format!("w{i}"))).unwrap();
        idx.push(ap.index);
        ap.durable.await.unwrap();
    }
    assert_eq!(idx.first(), Some(&Index(1)));
    assert_eq!(idx.last(), Some(&Index(10)));
    j.uploaded(&a, Epoch(1), Index(4)).await.unwrap();
    assert_eq!(j.pending().await.unwrap(), vec![(a.clone(), Index(10))]);
    j.stop();
    drop(j);
    // restart: records above the uploaded index come back, in order
    let j = LocalJournal::open_dir(dir.path(), Config::default()).unwrap();
    let o = j.open(&a, Epoch(2), Index(4)).await.unwrap();
    let got: Vec<u64> = o.replay.iter().map(|(i, _)| i.0).collect();
    assert_eq!(got, (5..=10).collect::<Vec<_>>());
    assert_eq!(o.replay[0].1, b("w4"));
    assert_eq!(o.next, Index(11));
}

#[tokio::test]
async fn marker_ahead_of_journal_and_older_epoch_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let a = DbKey::new("a", 7);
    let j = LocalJournal::open_dir(dir.path(), Config::default()).unwrap();
    j.open(&a, Epoch(5), Index(0)).await.unwrap();
    j.append(&a, Epoch(5), b("x")).unwrap().durable.await.unwrap();
    // a newer writer (epoch 6) opens with a marker ahead of the journal (S3 has unacknowledged records)
    let o = j.open(&a, Epoch(6), Index(3)).await.unwrap();
    assert!(o.replay.is_empty());
    assert_eq!(o.next, Index(4));
    // the old writer is fenced
    match j.append(&a, Epoch(5), b("late")) {
        Err(JournalError::Fenced { promised }) => assert_eq!(promised, Epoch(6)),
        other => panic!("expected Fenced, got {:?}", other.map(|x| x.index)),
    }
    assert!(matches!(j.open(&a, Epoch(4), Index(0)).await, Err(JournalError::Fenced { .. })));
    // the fence survives a restart
    j.stop();
    drop(j);
    let j = LocalJournal::open_dir(dir.path(), Config::default()).unwrap();
    assert!(matches!(j.open(&a, Epoch(5), Index(0)).await, Err(JournalError::Fenced { .. })));
}

#[tokio::test]
async fn torn_tail_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let a = DbKey::new("a", 1);
    let j = LocalJournal::open_dir(dir.path(), Config::default()).unwrap();
    j.open(&a, Epoch(1), Index(0)).await.unwrap();
    for i in 0..3 {
        j.append(&a, Epoch(1), b(&format!("w{i}"))).unwrap().durable.await.unwrap();
    }
    j.stop();
    drop(j);
    // half a frame at the end of the last segment (a crash during a write that was never acknowledged)
    let mut segs: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).filter(|p| p.extension().map_or(false, |e| e == "j3z") && p.file_name().unwrap().to_str().unwrap().starts_with("seg-")).collect();
    segs.sort();
    let mut f = std::fs::OpenOptions::new().append(true).open(segs.last().unwrap()).unwrap();
    f.write_all(&[200, 0, 0, 0, 1, 2, 3]).unwrap();
    drop(f);
    let j = LocalJournal::open_dir(dir.path(), Config::default()).unwrap();
    let o = j.open(&a, Epoch(2), Index(0)).await.unwrap();
    assert_eq!(o.replay.len(), 3);
}

#[tokio::test]
async fn erase_and_segment_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config { segment_max: 4096, db_hard_cap: 1 << 20 };
    let (a, c) = (DbKey::new("a", 1), DbKey::new("c", 1));
    let j = LocalJournal::open_dir(dir.path(), cfg.clone()).unwrap();
    j.open(&a, Epoch(1), Index(0)).await.unwrap();
    j.open(&c, Epoch(1), Index(0)).await.unwrap();
    let payload = Bytes::from(vec![7u8; 500]);
    let mut last_a = Index(0);
    for _ in 0..40 {
        last_a = j.append(&a, Epoch(1), payload.clone()).unwrap().index;
        j.append(&c, Epoch(1), payload.clone()).unwrap().durable.await.unwrap();
    }
    let before = j.segments();
    assert!(before > 3, "segments: {before}");
    // c is erased, a uploaded: every closed segment can go
    j.erase(&c).await.unwrap();
    j.uploaded(&a, Epoch(1), last_a).await.unwrap();
    assert!(j.segments() <= 2, "segments after trim: {}", j.segments());
    assert!(matches!(j.open(&c, Epoch(2), Index(0)).await, Err(JournalError::Erased)));
    assert!(j.pending().await.unwrap().is_empty());
    // the erase and the fences survive a restart even though the segments with the OPEN records are gone
    j.stop();
    drop(j);
    let j = LocalJournal::open_dir(dir.path(), cfg).unwrap();
    assert!(matches!(j.open(&c, Epoch(9), Index(0)).await, Err(JournalError::Erased)));
    let o = j.open(&a, Epoch(2), last_a).await.unwrap();
    assert!(o.replay.is_empty());
    assert_eq!(o.next, Index(last_a.0 + 1));
}

#[tokio::test]
async fn quota_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let a = DbKey::new("a", 1);
    let j = LocalJournal::open_dir(dir.path(), Config { segment_max: 64 << 20, db_hard_cap: 1000 }).unwrap();
    j.open(&a, Epoch(1), Index(0)).await.unwrap();
    let p = Bytes::from(vec![1u8; 400]);
    j.append(&a, Epoch(1), p.clone()).unwrap();
    j.append(&a, Epoch(1), p.clone()).unwrap();
    assert!(matches!(j.append(&a, Epoch(1), p.clone()), Err(JournalError::Quota)));
    assert!(matches!(j.close(&a, Epoch(1)).await, Err(JournalError::Invalid(_))));
    j.uploaded(&a, Epoch(1), Index(2)).await.unwrap();
    j.append(&a, Epoch(1), p.clone()).unwrap().durable.await.unwrap();
    j.uploaded(&a, Epoch(1), Index(3)).await.unwrap();
    j.close(&a, Epoch(1)).await.unwrap();
}
