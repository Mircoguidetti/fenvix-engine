"""Kitchen (K2c) · the per-size options of kitchen/command.json, checked in the workflow's "decide" job.
  K6 (reviewer of K2b): "server_options", one string per size with options for mongo-server on that cook, so the same
     m7i.large can be measured with the engine's default counter (block cache 256 MiB, metadata cache 64 MiB) and with a
     big counter (for example "--block-cache-mb 4096 --meta-cache-mb 1024"). The server does not change: these are
     options it already has. Only "--name value" or "--name" tokens; the options server.sh sets itself (store, bucket,
     listen, TLS, users, replica set, endpoint...) are refused, so a command can never point a cook at another bucket.
  K5: "prepare", true/false for every size, or one list with one value per size: before the round, open every database
     on the size's cook and wait for the pantry to calm down.
Usage: size_options.py SIZES_JSON SERVER_OPTIONS_JSON PREPARE_JSON
Prints two lines (compact JSON lists, one entry per size): the server options and the prepare flags. Exit 1 = refused,
with the reason on stdout."""
import json
import re
import sys

PREPARE_DEFAULT = False  # K2c local test: opening every database and settling changed nothing (report K2c, "K5"); a size can ask true
TOKEN = re.compile(r"^--[a-z][a-z0-9-]{1,40}$")
VALUE = re.compile(r"^[A-Za-z0-9._:-]{1,40}$")
# set by server.sh (or meaningless for a measured cook): never from a command
RESERVED = {"--listen", "--store", "--bucket", "--region", "--endpoint", "--local-dir", "--replica-set", "--advertise",
            "--auth", "--tls-mode", "--tls-certificate-key-file", "--tls-ca-file", "--holder", "--no-leases",
            "--unsafe-skip-ownership-checks", "--local-log-dir", "--log-commands"}


def no(msg):
    print(msg)
    sys.exit(1)


def per_size(value, n, name, check):
    if value is None:
        return None
    if not isinstance(value, list):
        value = [value] * n
    if len(value) != n:
        no(f"{name}: one entry per size is needed ({n} sizes, {len(value)} entries)")
    return [check(v) for v in value]


def check_options(s):
    if not isinstance(s, str) or len(s) > 300:
        no("server_options: every entry is a string of at most 300 characters")
    words = s.split()
    out, i = [], 0
    while i < len(words):
        w = words[i]
        if not TOKEN.match(w):
            no(f"server_options: {w!r} is not an option (--name or --name value)")
        if w in RESERVED:
            no(f"server_options: {w} is set by server.sh, not by a command")
        out.append(w)
        if i + 1 < len(words) and not words[i + 1].startswith("--"):
            if not VALUE.match(words[i + 1]):
                no(f"server_options: value {words[i + 1]!r} of {w} not allowed")
            out.append(words[i + 1])
            i += 1
        i += 1
    return " ".join(out)


def check_prepare(v):
    if not isinstance(v, bool):
        no("prepare: true or false (or a list with one per size)")
    return v


def main():
    sizes = json.loads(sys.argv[1])
    if isinstance(sizes, str):
        sizes = [sizes]
    n = len(sizes)
    opts = per_size(json.loads(sys.argv[2]), n, "server_options", check_options) or [""] * n
    prep = per_size(json.loads(sys.argv[3]), n, "prepare", check_prepare) or [PREPARE_DEFAULT] * n
    print(json.dumps(opts, separators=(",", ":")))
    print(json.dumps(prep, separators=(",", ":")))


if __name__ == "__main__":
    main()
