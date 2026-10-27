"""GridFS through pymongo's gridfs package: collections, document fields, indexes and the bucket/legacy APIs."""
import io
import gridfs
from probe_diff import case


def norm_files(db, bucket="fs"):
    files = []
    for f in db[bucket + ".files"].find().sort("filename", 1):
        f = dict(f)
        f["uploadDate"] = "<date>" if f.get("uploadDate") is not None else None
        files.append(f)
    chunks = [{k: v for k, v in c.items()} for c in db[bucket + ".chunks"].find().sort([("files_id", 1), ("n", 1)])]
    idx = {c: sorted((i["name"], tuple(i["key"].keys()), bool(i.get("unique"))) for i in db[c].list_indexes()) for c in (bucket + ".files", bucket + ".chunks")
           if c in db.list_collection_names()}
    return {"files": files, "chunks": chunks, "indexes": idx, "colls": sorted(c for c in db.list_collection_names() if c.startswith(bucket))}


PYIDS = ["py:gridfs.GridFSBucket.upload_from_stream", "py:gridfs.GridFSBucket.upload_from_stream_with_id", "py:gridfs.GridFSBucket.open_upload_stream", "py:gridfs.GridFSBucket.open_upload_stream_with_id", "py:gridfs.GridFSBucket.download_to_stream", "py:gridfs.GridFSBucket.open_download_stream", "py:gridfs.grid_file.GridIn.write", "py:gridfs.grid_file.GridIn.close", "py:gridfs.grid_file.GridOut.read", "py:gridfs.grid_file.GridOut.close"]
FILE_IDS = PYIDS + ["gridfs:fs.files collection", "gridfs:fs.chunks collection", "gridfs:files index (filename, uploadDate)", "gridfs:chunks unique index (files_id, n)",
            "gridfs:chunks._id", "gridfs:chunks.files_id", "gridfs:chunks.n", "gridfs:chunks.data", "gridfs:files._id", "gridfs:files.length", "gridfs:files.chunkSize",
            "gridfs:files.uploadDate", "gridfs:files.filename", "gridfs:files.metadata", "gridfs:files.md5"]


@case("compat.gridfs.bucket_upload_download", FILE_IDS)
def _bucket(client, db):
    b = gridfs.GridFSBucket(db, chunk_size_bytes=4)
    fid = b.upload_from_stream("a.txt", b"hello gridfs world", metadata={"k": 1})
    out = io.BytesIO()
    b.download_to_stream(fid, out)
    s = b.open_download_stream(fid)
    return {"bytes": out.getvalue(), "read": s.read(5), "length": s.length, "name": s.filename, "metadata": s.metadata, "chunk": s.chunk_size, "state": norm_files(db)}


@case("compat.gridfs.bucket_find_rename_delete", ["gridfs:files.contentType", "gridfs:files.aliases", "py:gridfs.GridFSBucket.find", "py:gridfs.GridFSBucket.rename", "py:gridfs.GridFSBucket.rename_by_name", "py:gridfs.GridFSBucket.delete", "py:gridfs.GridFSBucket.delete_by_name", "py:gridfs.GridFSBucket.open_download_stream_by_name", "py:gridfs.GridFSBucket.download_to_stream_by_name"])
def _ops(client, db):
    b = gridfs.GridFSBucket(db)
    ids = [b.upload_from_stream(f"f{i}.bin", bytes([i]) * 10, metadata={"i": i}) for i in range(3)]
    found = [f.filename for f in b.find({"metadata.i": {"$gte": 1}}).sort("filename", 1)]
    b.rename(ids[0], "renamed.bin")
    b.delete(ids[1])
    names = sorted(f.filename for f in b.find())
    try:
        b.open_download_stream(ids[1])
        missing = "found"
    except Exception as e:  # noqa
        missing = type(e).__name__
    by_name = b.open_download_stream_by_name("f2.bin").read()
    out2 = io.BytesIO(); b.download_to_stream_by_name("f2.bin", out2)
    b.rename_by_name("f2.bin", "f2r.bin")
    b.delete_by_name("renamed.bin")
    return {"found": found, "names": names, "deleted": missing, "by_name": by_name, "to_stream_by_name": out2.getvalue(), "after": sorted(f.filename for f in b.find())}


@case("compat.gridfs.bucket_custom_name", ["gridfs:fs.files collection"])
def _named(client, db):
    b = gridfs.GridFSBucket(db, bucket_name="photos")
    b.upload_from_stream("p.jpg", b"x" * 300000)
    return norm_files(db, "photos")


@case("compat.gridfs.legacy_api", ["py:gridfs.GridFS.put", "py:gridfs.GridFS.get", "py:gridfs.GridFS.get_last_version", "py:gridfs.GridFS.exists", "py:gridfs.GridFS.list", "py:gridfs.GridFS.delete"])
def _legacy(client, db):
    fs = gridfs.GridFS(db)
    fid = fs.put(b"legacy data", filename="l.txt", contentType="text/plain", aliases=["a", "b"])
    g = fs.get(fid)
    out = {"data": g.read(), "ct": g.content_type, "aliases": g.aliases, "exists": fs.exists(fid), "list": fs.list(), "last": fs.get_last_version("l.txt").read()}
    fs.delete(fid)
    out["after_delete"] = fs.exists(fid)
    out["state"] = norm_files(db)
    return out


@case("compat.gridfs.empty_file_and_large", ["gridfs:files.length", "py:gridfs.grid_file.GridOut.readchunk"])
def _large(client, db):
    b = gridfs.GridFSBucket(db)
    e = b.upload_from_stream("empty", b"")
    big = b.upload_from_stream("big", bytes(range(256)) * 4000)
    out = io.BytesIO(); b.download_to_stream(big, out)
    return {"empty": b.open_download_stream(e).read(), "big_len": len(out.getvalue()), "big_ok": out.getvalue() == bytes(range(256)) * 4000,
            "chunks": db.fs.chunks.count_documents({"files_id": big})}
