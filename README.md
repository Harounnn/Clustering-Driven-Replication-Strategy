# An Implementation of the paper: "A Clustering-Aware and Multi-Feature Replication Strategy"

This repository implements **“A Clustering-Aware and Multi-Feature Replication Strategy”**, providing feature extraction (log generation), clustering (KMeans ++), and replica-placement logic (Clustering Refinement) for distributed file systems. The project includes a runnable environment using Docker and a modular Python implementation.

---

## Usage

```bash
git clone https://github.com/Harounnn/Clustering-Driven-Replication-Strategy.git
cd Clustering-Driven-Replication-Strategy

python -m pip install --upgrade pip
pip install -r requirements.txt

# Starting the cluster
make up

# Simulation commands
make exec-nn

make exec-dn

make fs-ls

# Kill the cluster
make down
```