terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.60.0"
    }
  }
  required_version = ">= 1.5"
}

provider "hcloud" {
  token = var.hcloud_token
}

# ── SSH Key ───────────────────────────────────────────────────────────────────

data "hcloud_ssh_key" "deploy" {
  name = var.existing_ssh_key_name
}


# ── Private Network ───────────────────────────────────────────────────────────
resource "hcloud_network" "private" {
  name     = "${var.project_name}-net"
  ip_range = "10.0.0.0/16"
}

resource "hcloud_network_subnet" "private" {
  network_id   = hcloud_network.private.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = "10.0.1.0/24"
}

# ── Firewall ──────────────────────────────────────────────────────────────────
resource "hcloud_firewall" "stream" {
  name = "${var.project_name}-stream-fw"

  # SSH from anywhere (lock down to your IP in production)
  rule {
    direction = "in"
    protocol  = "tcp"
    port      = "22"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # Kafka broker reachable only from the private network.
  rule {
    direction = "in"
    protocol  = "tcp"
    port      = "9092"
    source_ips = ["10.0.1.0/24"]
  }

  # Allow all outbound
  rule {
    direction        = "out"
    protocol         = "tcp"
    port             = "any"
    destination_ips  = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction        = "out"
    protocol         = "udp"
    port             = "any"
    destination_ips  = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction        = "out"
    protocol         = "icmp"
    destination_ips  = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_firewall" "data" {
  name = "${var.project_name}-data-fw"

  # SSH
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # HTTP (Caddy redirect to HTTPS)
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # HTTPS (React dashboard, Grafana, FastAPI)
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # ClickHouse HTTP API reachable only from stream-1 over the private network.
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "8123"
    source_ips = ["10.0.1.0/24"]
  }

  # Allow all outbound
  rule {
    direction        = "out"
    protocol         = "tcp"
    port             = "any"
    destination_ips  = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction        = "out"
    protocol         = "udp"
    port             = "any"
    destination_ips  = ["0.0.0.0/0", "::/0"]
  }
  rule {
    direction        = "out"
    protocol         = "icmp"
    destination_ips  = ["0.0.0.0/0", "::/0"]
  }
}

# ── VMs ───────────────────────────────────────────────────────────────────────

# stream-1: Kafka (KRaft) + PySpark + Python producer
resource "hcloud_server" "stream" {
  name        = "${var.project_name}-stream-1"
  server_type = "cx43"           # 8 vCPU / 16 GB RAM — Kafka + Spark need this
  image       = "ubuntu-22.04"
  location    = var.location
  
  ssh_keys    = [data.hcloud_ssh_key.deploy.id]


  firewall_ids = [hcloud_firewall.stream.id]

  user_data = file("${path.module}/cloud-init-stream.yaml")

  labels = {
    project = var.project_name
    role    = "stream"
  }
}

resource "hcloud_server_network" "stream" {
  server_id  = hcloud_server.stream.id
  network_id = hcloud_network.private.id
  ip         = "10.0.1.10"
}

# data-1: ClickHouse + FastAPI + Grafana + Caddy + React static build
resource "hcloud_server" "data" {
  name        = "${var.project_name}-data-1"
  server_type = "cx33"           # 4 vCPU / 8 GB RAM — ClickHouse + services
  image       = "ubuntu-22.04"
  location    = var.location
  ssh_keys    = [data.hcloud_ssh_key.deploy.id]

  firewall_ids = [hcloud_firewall.data.id]

  user_data = file("${path.module}/cloud-init-data.yaml")

  labels = {
    project = var.project_name
    role    = "data"
  }
}

resource "hcloud_server_network" "data" {
  server_id  = hcloud_server.data.id
  network_id = hcloud_network.private.id
  ip         = "10.0.1.20"
}

# ── Attached Volumes ──────────────────────────────────────────────────────────

# ClickHouse data volume — survives VM rebuilds
resource "hcloud_volume" "clickhouse" {
  name      = "${var.project_name}-clickhouse-data"
  size      = 20          # GB — increase as your data grows
  location  = var.location
  format    = "ext4"
}

resource "hcloud_volume_attachment" "clickhouse" {
  volume_id = hcloud_volume.clickhouse.id
  server_id = hcloud_server.data.id
  automount = true
}

# Spark checkpoints on stream-1
resource "hcloud_volume" "spark" {
  name      = "${var.project_name}-spark-data"
  size      = 10
  location  = var.location
  format    = "ext4"
}

resource "hcloud_volume_attachment" "spark" {
  volume_id = hcloud_volume.spark.id
  server_id = hcloud_server.stream.id
  automount = true
}
