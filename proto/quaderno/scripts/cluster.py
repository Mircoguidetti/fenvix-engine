#!/usr/bin/env python3
"""Gruppo locale del prototipo: proxy, 3 repliche (3 "zone" simulate), MinIO, scaricatore.

Porte: replica i ascolta le altre repliche su 7100+i e i client su 7200+i. Il proxy espone
  r{a}->r{b} su 7300+10a+b (verso 7100+b): tutto il traffico tra repliche passa dal proxy;
  c->r{i}   su 7400+i      (verso 7200+i): i client che devono poter essere tagliati fuori passano dal proxy.
Cartella di lavoro: $PQ_WORK (default /root/pq-work), fuori dal repository.

  cluster.py up [--fresh] [--no-proxy] [--mem-cap-mb N] [--window-ms N] [--delay-ms D] [--disk-soft-mb N --disk-hard-mb N --disk-ro-mb N] [--alarm-s N]
     (--mem-cap-mb: tetto del quaderno per replica; qui le 3 repliche stanno sulla stessa macchina)
  cluster.py down | status | leader
  cluster.py kill I | start I | sigstop I | sigcont I
  cluster.py isolate I [--clients]   (blackhole tra I e le altre repliche; con --clients anche c->rI)
  cluster.py heal
  cluster.py minio-up | minio-down | minio-stop | minio-cont
  cluster.py flusher-up [args...] | flusher-down
  prova-zone:
  cluster.py up ... --copy [--copy-ms N]   copia del quaderno su S3 (attraverso la rotta s3 del proxy, porta 9100)
  cluster.py zone-kill        SIGKILL di tutte e 3 le repliche e dello scaricatore, poi cancellazione delle cartelle
  cluster.py restore          ripristino da zero in cartelle nuove (pq ripristina), poi avvio delle 3 repliche
  cluster.py s3 pass|blackhole|delay MS   stato della rotta s3 del proxy (copia e scaricatore passano da lì)
"""
import json, os, signal, subprocess, sys, time, socket

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.path.join(HERE, '..', 'target', 'release', 'pq')
WORK = os.environ.get('PQ_WORK', '/root/pq-work')
MINIO = os.environ.get('MINIO', '/root/pq-tools/bin/minio')
N = 3


def routes():
    r = []
    for a in range(N):
        for b in range(N):
            if a != b:
                r.append({'name': f'r{a}->r{b}', 'listen': f'127.0.0.1:{7300 + 10 * a + b}', 'target': f'127.0.0.1:{7100 + b}'})
    for i in range(N):
        r.append({'name': f'c->r{i}', 'listen': f'127.0.0.1:{7400 + i}', 'target': f'127.0.0.1:{7200 + i}'})
    # prova-zone: S3 dietro il proxy, per renderlo lento o irraggiungibile
    r.append({'name': 's3', 'listen': '127.0.0.1:9100', 'target': '127.0.0.1:9000'})
    return r


def pids():
    try:
        return json.load(open(os.path.join(WORK, 'pids.json')))
    except Exception:
        return {}


def save(p):
    json.dump(p, open(os.path.join(WORK, 'pids.json'), 'w'))


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def wait_port(port, t=10):
    end = time.time() + t
    while time.time() < end:
        try:
            socket.create_connection(('127.0.0.1', port), 0.2).close()
            return True
        except Exception:
            time.sleep(0.05)
    return False


def spawn(name, args, log):
    f = open(os.path.join(WORK, log), 'ab')
    p = subprocess.Popen(args, stdout=f, stderr=f, start_new_session=True)
    return p.pid


def replica_args(i, cfg):
    if cfg.get('no_proxy'):
        # giro 4: repliche collegate direttamente, senza proxy (prove di velocità e di CPU)
        peers = ','.join(f'{b}=127.0.0.1:{7100 + b}' for b in range(N) if b != i)
    else:
        peers = ','.join(f'{b}=127.0.0.1:{7300 + 10 * i + b}' for b in range(N) if b != i)
    return [PQ, 'replica', '--id', str(i), '--dir', os.path.join(WORK, f'r{i}'), '--peer-listen', f'127.0.0.1:{7100 + i}',
            '--client-listen', f'127.0.0.1:{7200 + i}', '--peers', peers, '--mem-cap-mb', str(cfg.get('mem_cap_mb', 4096)),
            '--window-ms', str(cfg.get('window_ms', 60000)), '--disk-soft-mb', str(cfg.get('disk_soft_mb', 0)),
            '--disk-hard-mb', str(cfg.get('disk_hard_mb', 0)), '--disk-ro-mb', str(cfg.get('disk_ro_mb', 0)),
            '--alarm-s', str(cfg.get('alarm_s', 120))] + (
            ['--copy-s3', 'http://127.0.0.1:9100', '--copy-ms', str(cfg.get('copy_ms', 1000))] if cfg.get('copy') else [])


def start_replica(i):
    p = pids()
    cfg = p.get('cfg', {})
    p[f'r{i}'] = spawn(f'r{i}', replica_args(i, cfg), f'r{i}.log')
    save(p)
    wait_port(7200 + i)


def ctl(m):
    tmp = os.path.join(WORK, 'ctl.json.tmp')
    json.dump(m, open(tmp, 'w'))
    os.rename(tmp, os.path.join(WORK, 'ctl.json'))
    time.sleep(0.15)


def base_ctl():
    try:
        return json.load(open(os.path.join(WORK, 'base_ctl.json')))
    except Exception:
        return {}


def status():
    out = subprocess.run([PQ, 'status'], capture_output=True, text=True, timeout=20).stdout
    return [json.loads(l) for l in out.splitlines() if l.strip()]


def leader():
    for _ in range(100):
        for s in status():
            if s.get('role') == 'Leader':
                return s['id']
        time.sleep(0.1)
    return -1


def opt(args, k, d=None):
    if k in args:
        return args[args.index(k) + 1]
    return d


def main():
    a = sys.argv[1:]
    cmd = a[0]
    os.makedirs(WORK, exist_ok=True)
    if cmd == 'up':
        down()
        if '--fresh' in a:
            for i in range(N):
                subprocess.run(['rm', '-rf', os.path.join(WORK, f'r{i}'), os.path.join(WORK, f'r{i}.log')])
        cfg = {'mem_cap_mb': int(opt(a, '--mem-cap-mb', 4096)), 'window_ms': int(opt(a, '--window-ms', 60000)),
               'disk_soft_mb': int(opt(a, '--disk-soft-mb', 0)), 'disk_hard_mb': int(opt(a, '--disk-hard-mb', 0)),
               'disk_ro_mb': int(opt(a, '--disk-ro-mb', 0)), 'alarm_s': int(opt(a, '--alarm-s', 120)),
               'no_proxy': '--no-proxy' in a, 'copy': '--copy' in a, 'copy_ms': int(opt(a, '--copy-ms', 1000))}
        p = {'cfg': cfg}
        save(p)
        json.dump(routes(), open(os.path.join(WORK, 'routes.json'), 'w'))
        d = int(opt(a, '--delay-ms', 0))
        bc = {r['name']: {'mode': 'pass', 'delay_ms': d} for r in routes() if r['name'].startswith('r')} if d else {}
        json.dump(bc, open(os.path.join(WORK, 'base_ctl.json'), 'w'))
        ctl(bc)
        p['proxy'] = spawn('proxy', [PQ, 'proxy', '--routes', os.path.join(WORK, 'routes.json'), '--ctl', os.path.join(WORK, 'ctl.json')], 'proxy.log')
        save(p)
        wait_port(7301)
        for i in range(N):
            start_replica(i)
        print('leader', leader())
    elif cmd == 'down':
        down()
    elif cmd == 'status':
        for s in status():
            print(json.dumps(s))
    elif cmd == 'leader':
        print(leader())
    elif cmd in ('kill', 'sigstop', 'sigcont'):
        i = int(a[1])
        sig = {'kill': signal.SIGKILL, 'sigstop': signal.SIGSTOP, 'sigcont': signal.SIGCONT}[cmd]
        os.kill(pids()[f'r{i}'], sig)
    elif cmd == 'start':
        start_replica(int(a[1]))
    elif cmd == 'isolate':
        i = int(a[1])
        m = base_ctl()
        for b in range(N):
            if b != i:
                m[f'r{i}->r{b}'] = {'mode': 'blackhole'}
                m[f'r{b}->r{i}'] = {'mode': 'blackhole'}
        if '--clients' in a:
            m[f'c->r{i}'] = {'mode': 'blackhole'}
        ctl(m)
    elif cmd == 'heal':
        ctl(base_ctl())
    elif cmd == 'zone-kill':
        p = pids()
        for k in ['r0', 'r1', 'r2', 'flusher']:
            if k in p:
                try:
                    os.kill(p[k], signal.SIGCONT)
                    os.kill(p[k], signal.SIGKILL)
                except Exception:
                    pass
        # istante della morte (ms dall'epoca), dopo l'ultimo SIGKILL
        print(int(time.time() * 1000))
        time.sleep(0.3)
        # la zona è morta: anche i dischi (cartelle del registro e della dispensa) spariscono
        for i in range(N):
            subprocess.run(['rm', '-rf', os.path.join(WORK, f'r{i}')])
    elif cmd == 'restore':
        dirs = ','.join(os.path.join(WORK, f'r{i}') for i in range(N))
        t0 = time.time()
        out = subprocess.run([PQ, 'ripristina', '--s3', 'http://127.0.0.1:9100', '--dirs', dirs], capture_output=True, text=True)
        print(out.stdout.strip())
        if out.returncode != 0:
            print(out.stderr, file=sys.stderr)
            sys.exit(1)
        t1 = time.time()
        for i in range(N):
            start_replica(i)
        l = leader()
        print(json.dumps({'ripristino_s': round(t1 - t0, 3), 'repliche_e_capo_s': round(time.time() - t1, 3), 'capo': l}))
    elif cmd == 's3':
        m = base_ctl()
        mode = a[1]
        if mode == 'delay':
            m['s3'] = {'mode': 'pass', 'delay_ms': int(a[2])}
        else:
            m['s3'] = {'mode': mode}
        bc = dict(m)
        json.dump(bc, open(os.path.join(WORK, 'base_ctl.json'), 'w'))
        ctl(m)
    elif cmd == 'minio-up':
        p = pids()
        os.makedirs(os.path.join(WORK, 'minio'), exist_ok=True)
        env = dict(os.environ, MINIO_ROOT_USER='minioadmin', MINIO_ROOT_PASSWORD='minioadmin')
        f = open(os.path.join(WORK, 'minio.log'), 'ab')
        if os.path.exists(MINIO):
            p['minio'] = subprocess.Popen([MINIO, 'server', os.path.join(WORK, 'minio'), '--address', '127.0.0.1:9000', '--console-address', '127.0.0.1:9001'],
                                          stdout=f, stderr=f, env=env, start_new_session=True).pid
        else:
            # senza il binario di MinIO (ambiente senza rete verso dl.min.io): moto, server S3 locale in Python, con PUT
            # condizionali (If-None-Match / If-Match) come MinIO. I corpi vanno su file temporanei, non in RAM.
            tmp = os.path.join(WORK, 'minio', 'tmp')
            os.makedirs(tmp, exist_ok=True)
            env.update(MOTO_S3_DEFAULT_KEY_BUFFER_SIZE='1024', TMPDIR=tmp)
            p['minio'] = subprocess.Popen(['moto_server', '-H', '127.0.0.1', '-p', '9000'],
                                          stdout=f, stderr=f, env=env, start_new_session=True).pid
        save(p)
        wait_port(9000, 30)
        time.sleep(1)
        subprocess.run(['curl', '-s', '-o', '/dev/null', '--aws-sigv4', 'aws:amz:us-east-1:s3', '--user', 'minioadmin:minioadmin', '-X', 'PUT', 'http://127.0.0.1:9000/pq-sst'], env=dict(os.environ, NO_PROXY='*', no_proxy='*'))
    elif cmd in ('minio-stop', 'minio-cont', 'minio-down'):
        sig = {'minio-stop': signal.SIGSTOP, 'minio-cont': signal.SIGCONT, 'minio-down': signal.SIGKILL}[cmd]
        pid = pids().get('minio')
        if pid:
            if cmd == 'minio-down':
                os.kill(pid, signal.SIGCONT)
            os.kill(pid, sig)
    elif cmd == 'flusher-up':
        p = pids()
        p['flusher'] = spawn('flusher', [PQ, 'flusher'] + a[1:], 'flusher.log')
        save(p)
    elif cmd == 'flusher-down':
        pid = pids().get('flusher')
        if pid and alive(pid):
            os.kill(pid, signal.SIGKILL)
    else:
        print(__doc__)
        sys.exit(2)


def down():
    p = pids()
    for k, v in p.items():
        if k == 'cfg':
            continue
        try:
            os.kill(v, signal.SIGCONT)
            os.kill(v, signal.SIGKILL)
        except Exception:
            pass
    save({})
    time.sleep(0.3)


if __name__ == '__main__':
    main()
