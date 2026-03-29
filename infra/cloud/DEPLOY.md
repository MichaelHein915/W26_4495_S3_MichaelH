# Cloud Deployment Guide

Hetzner Cloud · 2 VMs · ~$35–40/month
Stack: Kafka (KRaft) · PySpark · ClickHouse · FastAPI · React · Grafana · Caddy

---

## Architecture

```
Coinbase WebSocket
  └── Python producer
        └── Kafka (KRaft, port 9092, private network)
              └── PySpark Structured Streaming
                    ├── checkpoints → /data/checkpoints  (attached volume)
                    ├── raw trades → ClickHouse raw_trades
                    └── candles / metrics / alerts → ClickHouse (10.0.1.20:8123)

stream-1 (10.0.1.10)          data-1 (10.0.1.20)
─────────────────────          ─────────────────────────────────────
Kafka KRaft                    ClickHouse   :8123 (localhost + private net)
Python producer                             :9000 (private net only)
PySpark                        FastAPI      :8000 (localhost only)
                               Grafana      :3000 (localhost only)
                               Caddy        :80 (public by default)
                                 ├── /grafana → grafana:3000
                                 ├── /api    → api:8000
                                 └── /       → api:8000 (React SPA)
```

---

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5
- [Docker](https://docs.docker.com/engine/install/) + Docker Compose v2 on your local machine
- A Hetzner Cloud account — [console.hetzner.cloud](https://console.hetzner.cloud)
- An SSH key pair (`ssh-keygen -t ed25519 -C "crypto-deploy"`)

A domain is optional for the first deployment. The stack can run over plain
HTTP on the data VM public IP. You can add a real domain or `nip.io` later.

---

## Step 1 — Provision infrastructure with Terraform

```bash
cd infra/cloud/terraform

# Copy and fill in your values
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: hcloud_token, ssh_public_key, domain

terraform init
terraform plan
terraform apply
```

After apply, Terraform prints the public IPs of both VMs and the next-step instructions.

If you do not have a domain yet, you can leave a placeholder value in
`terraform.tfvars` such as `placeholder.example.com`. Terraform only needs a
syntactically valid string there.

---

## Step 2 — Generate Kafka Cluster ID

KRaft requires a stable cluster ID. Generate it once:

```bash
docker run --rm apache/kafka:3.7.1 \
  /opt/kafka/bin/kafka-storage.sh random-uuid
```

Copy the output into `.env.cloud` as `KAFKA_CLUSTER_ID=...`

---

## Step 3 — Prepare environment files

```bash
cd infra/cloud
cp .env.cloud.example .env.cloud
# Edit .env.cloud — fill in the public URL, passwords, and KAFKA_CLUSTER_ID
```

**Required changes in `.env.cloud` for the first deploy:**
- `SITE_ADDRESS=:80`
- `PUBLIC_BASE_URL=http://<DATA_VM_PUBLIC_IP>`
- `KAFKA_CLUSTER_ID` → the UUID from Step 2
- `CLICKHOUSE_PASSWORD` → a strong password
- `GRAFANA_ADMIN_PASSWORD` → a strong password

If you later want automatic HTTPS, change:
- `SITE_ADDRESS` → your real domain or `<ip>.nip.io`
- `PUBLIC_BASE_URL` → `https://...`

---

## Step 4 — Build the React frontend

```bash
cd apps/dashboard/frontend
npm install
npm run build
# This creates apps/dashboard/frontend/dist/ — baked into the API Docker image
```

---

## Step 5 — Deploy stream-1 (Kafka + Spark + Producer)

```bash
# Copy files to stream-1
STREAM_IP=$(terraform -chdir=infra/cloud/terraform output -raw stream_1_public_ip)

rsync -av --exclude='.git' --exclude='.venv' --exclude='node_modules' \
  . root@$STREAM_IP:/opt/crypto-analytics/

# SSH in and start services
ssh root@$STREAM_IP
cd /opt/crypto-analytics
docker compose -f infra/cloud/stream-1/docker-compose.yml up -d --build

# Verify
docker compose -f infra/cloud/stream-1/docker-compose.yml ps
docker compose -f infra/cloud/stream-1/docker-compose.yml logs -f kafka
```

---

## Step 6 — Deploy data-1 (ClickHouse + API + Grafana + Caddy)

```bash
DATA_IP=$(terraform -chdir=infra/cloud/terraform output -raw data_1_public_ip)

rsync -av --exclude='.git' --exclude='.venv' --exclude='node_modules' \
  . root@$DATA_IP:/opt/crypto-analytics/

ssh root@$DATA_IP
cd /opt/crypto-analytics
docker compose -f infra/cloud/data-1/docker-compose.yml up -d --build

# Verify
docker compose -f infra/cloud/data-1/docker-compose.yml ps
docker compose -f infra/cloud/data-1/docker-compose.yml logs -f caddy
```

---

## Step 7 — Verify the deployment

```bash
# API health
curl http://<DATA_VM_PUBLIC_IP>/api/health

# Dashboard
open http://<DATA_VM_PUBLIC_IP>/

# Grafana (admin / your GRAFANA_ADMIN_PASSWORD)
open http://<DATA_VM_PUBLIC_IP>/grafana
```

ClickHouse tables should start filling within 60–90 seconds of the producer connecting to Coinbase.

---

## Runtime notes

- The cloud pipeline writes raw trades, candles, live metrics, and alerts into ClickHouse.
- The React dashboard keeps using `/api/dashboard/stream`; the API polls ClickHouse and emits fresh snapshots over SSE.
- The default deployment is plain HTTP on the server IP. Add a domain later only if you want automatic HTTPS.

---

## Useful commands

```bash
# Tail all logs on stream-1
docker compose -f infra/cloud/stream-1/docker-compose.yml logs -f

# Tail all logs on data-1
docker compose -f infra/cloud/data-1/docker-compose.yml logs -f

# Check ClickHouse data
docker exec -it clickhouse clickhouse-client \
  --query "SELECT product_id, count() FROM crypto.candles_1m GROUP BY product_id"

# Restart a single service
docker compose -f infra/cloud/stream-1/docker-compose.yml restart spark

# Tear down everything (VMs and volumes persist — only containers stop)
docker compose -f infra/cloud/stream-1/docker-compose.yml down
docker compose -f infra/cloud/data-1/docker-compose.yml down

# Destroy all infrastructure (WARNING: deletes VMs and volumes)
terraform -chdir=infra/cloud/terraform destroy
```

---

## Cost breakdown (Hetzner)

| Resource | Spec | Cost/mo |
|---|---|---|
| stream-1 | CX41 — 4 vCPU / 16 GB | ~€19 |
| data-1 | CX31 — 2 vCPU / 8 GB | ~€11 |
| spark-data volume | 10 GB | ~€0.50 |
| clickhouse-data volume | 20 GB | ~€1.00 |
| Network / bandwidth | Generous free tier | ~€0 |
| **Total** | | **~€31–33/mo (~$35–38)** |

---

## Zoomcamp submission checklist

- [x] Cloud deployment (Hetzner VMs)
- [x] Infrastructure as Code (Terraform — `infra/cloud/terraform/`)
- [x] Streaming ingestion (Kafka KRaft + PySpark Structured Streaming)
- [x] Data warehouse with partitioning (`PARTITION BY toYYYYMMDD(window_start)`, `ORDER BY (product_id, window_start)`)
- [x] Transformations (PySpark: VWAP, volatility, windowed aggs, volume alerts)
- [x] Dashboard — Grafana (VWAP time-series + volume by symbol bar chart — 2 panels minimum)
- [x] Reproducibility — this README + cloud Docker Compose files + `.env.cloud.example`
