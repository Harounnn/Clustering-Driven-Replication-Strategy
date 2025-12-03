import argparse, os, random, subprocess, csv, tempfile, shutil
from datetime import datetime, timedelta

def check_hdfs_cli():
    from shutil import which
    if which("hdfs") is None:
        raise EnvironmentError("hdfs CLI not found in PATH. Run inside a container that has Hadoop client installed.")

def hdfs_put_local_to_hdfs(local_path, hdfs_path):
    subprocess.check_call(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path])

def make_local_file(path, size):
    with open(path, "wb") as f:
        f.write(os.urandom(size))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=200, help="Number of files to create")
    parser.add_argument("--hdfs_dir", required=True)
    parser.add_argument("--min_size", type=int, default=1024)
    parser.add_argument("--max_size", type=int, default=1024*1024)
    parser.add_argument("--nodes", type=str, default="dn1,dn2,dn3")
    parser.add_argument("--age_days_max", type=int, default=365)
    parser.add_argument("--out_manifest", default="metadata.csv")
    args = parser.parse_args()

    check_hdfs_cli()
    nodes = args.nodes.split(",")
    tmpdir = tempfile.mkdtemp(prefix="synth_")
    manifest = []

    try:
        for i in range(args.n):
            size = random.randint(args.min_size, args.max_size)
            localfile = os.path.join(tmpdir, f"synth_{i}.bin")
            make_local_file(localfile, size)
            hdfs_path = os.path.join(args.hdfs_dir, f"synth_{i}.bin")
            print("Putting", localfile, "->", hdfs_path)
            hdfs_put_local_to_hdfs(localfile, hdfs_path)

            delta = random.random() * args.age_days_max
            creation = datetime.utcnow() - timedelta(days=delta)

            primary_node = random.choice(nodes)
            category = random.choices(["hot","shared","moderate","archival"], weights=[0.10,0.20,0.50,0.20])[0]

            manifest.append({
                "path": hdfs_path,
                "creation_ts": creation.isoformat()+"Z",
                "primary_node": primary_node,
                "size_bytes": size,
                "category": category
            })
    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass

    with open(args.out_manifest, "w", newline="") as csvf:
        w = csv.DictWriter(csvf, fieldnames=["path","creation_ts","primary_node","size_bytes","category"])
        w.writeheader()
        for r in manifest:
            w.writerow(r)

    print("Wrote", args.out_manifest)
    print("Done.")
if __name__ == "__main__":
    main()
