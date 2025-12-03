import argparse, csv, random, os
from datetime import datetime, timedelta
import math

def now_iso_ms(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  

def load_manifest(path):
    rows = []
    with open(path) as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def generate_events_for_file(file_record, out_queue, rates, clients, sim_start):
    path = file_record['path']
    duration = rates.get('duration', 60)
    lambda_rate = max(0.0, rates.get('read_rate', 0.1) + rates.get('write_rate', 0.01))
    if lambda_rate <= 0:
        return

    t = 0.0
    while t < duration:
        inter = random.expovariate(lambda_rate)
        t += inter
        if t >= duration:
            break

        p_read = rates.get('read_rate', 0.0) / (lambda_rate + 1e-12)
        op = "READ" if random.random() < p_read else "WRITE"

        if random.random() < rates.get('locality_bias', 0.5):
            client_node = file_record.get('primary_node', random.choice(clients))
        else:
            client_node = random.choice(clients)
        pid = random.randint(1000, 9999)
        ts = sim_start + timedelta(seconds=t)
        out_queue.append((now_iso_ms(ts), path, op, client_node, pid))

def generate_all(manifest, out_log, duration_seconds, clients):
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
        rates = dict(category_map.get(cat, category_map["moderate"]))
        rates['duration'] = duration_seconds

        rates['read_rate'] = max(0.0, random.gauss(rates['read_rate'], max(0.0001, rates['read_rate']*0.2)))
        rates['write_rate'] = max(0.0, random.gauss(rates['write_rate'], max(0.0001, rates['write_rate']*0.5)))
        rates['locality_bias'] = min(1.0, max(0.0, random.gauss(rates['locality_bias'], 0.2)))
        generate_events_for_file(rec, out_entries, rates, clients, sim_start)

    out_entries.sort(key=lambda x: x[0])
    with open(out_log, "w") as f:
        for ts, path, op, client, pid in out_entries:
            f.write(f"{ts},{path},{op},{client},{pid}\n")
    print("Wrote", out_log, "with", len(out_entries), "entries")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out", default="access.log")
    parser.add_argument("--duration_seconds", type=int, default=300, help="Simulated period in seconds")
    parser.add_argument("--clients", default="dn1,dn2,dn3,dn4", help="Comma separated client node ids")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    clients = args.clients.split(",")
    generate_all(manifest, args.out, args.duration_seconds, clients)
