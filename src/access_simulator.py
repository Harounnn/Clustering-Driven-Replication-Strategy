import argparse, csv, random, time, os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def now_iso():
    return datetime.utcnow().isoformat()+"Z"

def load_manifest(path):
    rows = []
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def client_worker(file_record, out_queue, rates, clients, sim_start_ts, speedup=1.0):
    path = file_record['path']
    read_rate = rates.get('read_rate', 0.1)
    write_rate = rates.get('write_rate', 0.01)
    total_seconds = rates.get('duration', 60)
    events = []
    for sec in range(int(total_seconds)):
        n_reads = random.poissonvariate(read_rate) if hasattr(random,'poissonvariate') else \
                  sum(1 for _ in range(int(round(read_rate))) )  # fallback coarse
        n_writes = random.poissonvariate(write_rate) if hasattr(random,'poissonvariate') else \
                   sum(1 for _ in range(int(round(write_rate))) )
        if random.random() < read_rate:
            events.append(('READ', sec))
        if random.random() < write_rate:
            events.append(('WRITE', sec))
    for op, sec in events:
        ts = sim_start_ts + timedelta(seconds=sec)
        client_node = random.choice(clients)
        if random.random() < rates.get('locality_bias', 0.5):
            client_node = file_record.get('primary_node', client_node)
        pid = random.randint(1000, 9999)
        out_queue.append((ts.isoformat()+"Z", file_record['path'], op, client_node, pid))

def generate_all(manifest, out_log, duration_seconds, clients, speedup=100.0):
    category_map = {
        "hot": {"read_rate":0.8, "write_rate":0.2, "locality_bias":0.7},
        "shared": {"read_rate":0.6, "write_rate":0.02, "locality_bias":0.3},
        "moderate": {"read_rate":0.1, "write_rate":0.01, "locality_bias":0.5},
        "archival": {"read_rate":0.005, "write_rate":0.001, "locality_bias":0.9}
    }
    sim_start = datetime.utcnow()
    out_entries = []
    for rec in manifest:
        cat = rec.get("category","moderate")
        rates = category_map.get(cat, category_map["moderate"])
        rates['duration'] = duration_seconds
        rates['read_rate'] = max(0.0, random.gauss(rates['read_rate'], rates['read_rate']*0.2))
        rates['write_rate'] = max(0.0, random.gauss(rates['write_rate'], max(0.001, rates['write_rate']*0.5)))
        rates['locality_bias'] = min(1.0,max(0.0, random.gauss(rates['locality_bias'],0.2)))
        generate_events_for_file(rec, out_entries, rates, clients, sim_start)
    out_entries.sort(key=lambda x: x[0])
    with open(out_log, "w") as f:
        for ts, path, op, client, pid in out_entries:
            f.write(f"{ts},{path},{op},{client},{pid}\n")
    print("Wrote", out_log, "with", len(out_entries), "entries")

def generate_events_for_file(file_record, out_queue, rates, clients, sim_start):
    total_seconds = int(rates['duration'])
    for sec in range(total_seconds):
        base_p = rates['read_rate'] + rates['write_rate']
        events_this_second = 0
        for c in clients:
            if random.random() < base_p:
                op = "READ" if random.random() < (rates['read_rate']/(rates['read_rate']+rates['write_rate']+1e-9)) else "WRITE"
                client_node = c
                if random.random() < rates['locality_bias']:
                    client_node = file_record.get('primary_node', client_node)
                pid = random.randint(1000, 9999)
                ts = sim_start + timedelta(seconds=sec, milliseconds=random.randint(0,999))
                out_queue.append((ts.isoformat()+"Z", file_record['path'], op, client_node, pid))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out", default="access.log")
    parser.add_argument("--duration_seconds", type=int, default=300, help="Simulated period in seconds")
    parser.add_argument("--clients", default="dn1,dn2,dn3,dn4", help="Comma separated client node ids")
    args = parser.parse_args()

    manifest = []
    with open(args.manifest) as f:
        r = csv.DictReader(f)
        for row in r:
            manifest.append(row)

    clients = args.clients.split(",")
    entries = []
    generate_all(manifest, args.out, args.duration_seconds, clients)
