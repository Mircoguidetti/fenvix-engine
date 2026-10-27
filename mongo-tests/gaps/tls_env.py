#!/usr/bin/env python3
"""Certificates and servers for the TLS tests: self-signed material generated here (python `cryptography`), real mongod 7.0.45 and
the engine started side by side on the same certificates.

    tls_env.py start | stop | certs

Pairs (real port / engine port):  A 27040/27048 requireTLS, CA, client certificates optional   B 27041/27049 requireTLS, self-signed
server certificate that is its own CA (mongod 7 refuses TLS without a chain of trust)   C 27042/27050 requireTLS, CA, client certificate required   D 27043/27051 preferTLS (plain and TLS)
E 27044/27052 like C with --auth (x.509 users in $external).   Certificates: .tools/certs/*.pem"""
import datetime, ipaddress, os, subprocess, sys, glob, time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CERTS = os.path.join(ROOT, ".tools", "certs")
DATA = os.path.join(ROOT, ".tools", "data")
MONGOD = glob.glob(os.path.join(ROOT, ".tools", "mongodb-linux-*", "bin", "mongod"))[0]
ENGINE = os.environ.get("ENGINE_BIN", os.path.join(ROOT, "target", "release", "mongo-server"))


def gen_certs():
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    os.makedirs(CERTS, exist_ok=True)

    def key():
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def name(cn, o="Test", ou=None, c="US"):
        attrs = [x509.NameAttribute(NameOID.COUNTRY_NAME, c), x509.NameAttribute(NameOID.ORGANIZATION_NAME, o)]
        if ou:
            attrs.append(x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, ou))
        attrs.append(x509.NameAttribute(NameOID.COMMON_NAME, cn))
        return x509.Name(attrs)

    def cert(subject, k, issuer_name, issuer_key, ca=False, san=None, client=False, server=False):
        now = datetime.datetime.now(datetime.timezone.utc)
        b = (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer_name).public_key(k.public_key()).serial_number(x509.random_serial_number())
             .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=3650))
             .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
        if san:
            b = b.add_extension(x509.SubjectAlternativeName(san), critical=False)
        ekus = ([x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH] if client else []) + ([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH] if server else [])
        if ekus:
            b = b.add_extension(x509.ExtendedKeyUsage(ekus), critical=False)
        return b.sign(issuer_key, hashes.SHA256())

    def pem(path, *objs):
        with open(os.path.join(CERTS, path), "wb") as f:
            for o in objs:
                if hasattr(o, "public_bytes"):
                    f.write(o.public_bytes(serialization.Encoding.PEM))
                else:
                    f.write(o.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))

    san = [x509.DNSName("localhost"), x509.DNSName("node1.example.test"), x509.DNSName("node2.example.test"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
    cak = key(); ca = cert(name("Test CA"), cak, name("Test CA"), cak, ca=True)
    pem("ca.pem", ca)
    sk = key(); sc = cert(name("localhost"), sk, ca.subject, cak, san=san, server=True); pem("server.pem", sc, sk)
    ck = key(); cc = cert(name("client1", o="Clients", ou="Dev"), ck, ca.subject, cak, client=True); pem("client.pem", cc, ck)
    c2k = key(); cc2 = cert(name("client2", o="Clients", ou="Dev"), c2k, ca.subject, cak, client=True); pem("client2.pem", cc2, c2k)
    ssk = key(); ss = cert(name("localhost"), ssk, name("localhost"), ssk, san=san, server=True); pem("selfsigned.pem", ss, ssk)
    ok = key(); oca = cert(name("Other CA"), ok, name("Other CA"), ok, ca=True); pem("other-ca.pem", oca)
    uk = key(); uc = cert(name("client1", o="Clients", ou="Dev"), uk, oca.subject, ok, client=True); pem("untrusted-client.pem", uc, uk)
    wk = key(); wc = cert(name("wronghost"), wk, ca.subject, cak, san=[x509.DNSName("other.example.test")], server=True); pem("wronghost.pem", wc, wk)
    print("certificates in", CERTS)


PAIRS = {
    "A": (27040, 27048, ["--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "server.pem", "--tlsCAFile", "ca.pem", "--tlsAllowConnectionsWithoutCertificates"],
          ["--tls-mode", "requireTLS", "--tls-certificate-key-file", "server.pem", "--tls-ca-file", "ca.pem", "--tls-allow-connections-without-certificates"]),
    "B": (27041, 27049, ["--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "selfsigned.pem", "--tlsCAFile", "selfsigned.pem", "--tlsAllowConnectionsWithoutCertificates"],
          ["--tls-mode", "requireTLS", "--tls-certificate-key-file", "selfsigned.pem", "--tls-ca-file", "selfsigned.pem", "--tls-allow-connections-without-certificates"]),
    "C": (27042, 27050, ["--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "server.pem", "--tlsCAFile", "ca.pem"],
          ["--tls-mode", "requireTLS", "--tls-certificate-key-file", "server.pem", "--tls-ca-file", "ca.pem"]),
    "D": (27043, 27051, ["--tlsMode", "preferTLS", "--tlsCertificateKeyFile", "server.pem", "--tlsCAFile", "ca.pem", "--tlsAllowConnectionsWithoutCertificates"],
          ["--tls-mode", "preferTLS", "--tls-certificate-key-file", "server.pem", "--tls-ca-file", "ca.pem", "--tls-allow-connections-without-certificates"]),
    "E": (27044, 27052, ["--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "server.pem", "--tlsCAFile", "ca.pem", "--auth"],
          ["--tls-mode", "requireTLS", "--tls-certificate-key-file", "server.pem", "--tls-ca-file", "ca.pem", "--auth"]),
}


def sh(c, **kw):
    return subprocess.run(c, shell=True, check=True, **kw)


def stop():
    subprocess.run("pkill -f '[d]ns_srv.py'", shell=True)
    for k, (rp, op, _, _) in PAIRS.items():
        subprocess.run(f"pkill -u mongotest -f 'port {rp} '; pkill -f '[l]isten 127.0.0.1:{op} '", shell=True)
    time.sleep(2)


def start():
    gen_certs() if not os.path.exists(os.path.join(CERTS, "server.pem")) else None
    stop()
    sh(f"chown -R mongotest {CERTS}")
    for k, (rp, op, mflags, eflags) in PAIRS.items():
        d = os.path.join(DATA, f"tls{k}")
        sh(f"rm -rf {d}; mkdir -p {d}; chown -R mongotest {d}")
        mf = " ".join(f"{CERTS}/{a}" if a.endswith(".pem") else a for a in mflags)
        sh(f"su mongotest -c 'ulimit -n 20000; {MONGOD} --port {rp} --dbpath {d} --bind_ip 127.0.0.1 {mf} --fork --logpath {d}.log' > /dev/null")
        ef = " ".join(f"{CERTS}/{a}" if a.endswith(".pem") else a for a in eflags)
        store = f"/tmp/gaps-tls{k}-store"
        sh(f"rm -rf {store}; nohup {ENGINE} --listen 127.0.0.1:{op} --store local --local-dir {store} --no-leases --idle-secs 600 --holder tls{k} {ef} > {ROOT}/mongo-tests/out/server-{op}.log 2>&1 &")
    time.sleep(3)
    hosts = open("/etc/hosts").read()
    for h in ("node1.example.test", "node2.example.test", "node3.example.test", "node9.example.test"):
        if h not in hosts:
            open("/etc/hosts", "a").write(f"127.0.0.1 {h}\n")
    from pymongo import MongoClient
    for port in (PAIRS["E"][0], PAIRS["E"][1]):
        c = MongoClient("localhost", port, tls=True, tlsCAFile=os.path.join(CERTS, "ca.pem"), tlsCertificateKeyFile=os.path.join(CERTS, "client.pem"), serverSelectionTimeoutMS=8000)
        c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    subprocess.run("pkill -f '[d]ns_srv.py'", shell=True)
    subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "dns_srv.py")], stdout=open(os.path.join(ROOT, "mongo-tests", "out", "dns.log"), "w"), stderr=subprocess.STDOUT, start_new_session=True)
    print("TLS servers up:", {k: (v[0], v[1]) for k, v in PAIRS.items()})


if __name__ == "__main__":
    {"start": start, "stop": stop, "certs": gen_certs}[sys.argv[1] if len(sys.argv) > 1 else "start"]()
