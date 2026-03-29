output "stream_1_public_ip" {
  description = "Public IP of stream-1 (Kafka + Spark). SSH: ssh root@<ip>"
  value       = hcloud_server.stream.ipv4_address
}

output "data_1_public_ip" {
  description = "Public IP of data-1 (ClickHouse + Grafana + App). Point your domain A record here."
  value       = hcloud_server.data.ipv4_address
}

output "stream_1_private_ip" {
  value = "10.0.1.10"
}

output "data_1_private_ip" {
  value = "10.0.1.20"
}

output "next_steps" {
  value = <<-EOT
    ── Next steps ──────────────────────────────────────────────────────
    1. Point your domain A record → ${hcloud_server.data.ipv4_address}
    2. SSH into stream-1:  ssh root@${hcloud_server.stream.ipv4_address}
    3. SSH into data-1:    ssh root@${hcloud_server.data.ipv4_address}
    4. On stream-1: cd /opt/crypto-analytics && docker compose --env-file infra/cloud/.env.cloud -f infra/cloud/stream-1/docker-compose.yml up -d --build
    5. On data-1:   cd /opt/crypto-analytics && docker compose --env-file infra/cloud/.env.cloud -f infra/cloud/data-1/docker-compose.yml up -d --build
    5. Dashboard: https://<your-domain>/
    6. Grafana:   https://<your-domain>/grafana
    7. API:       https://<your-domain>/api/health
    ────────────────────────────────────────────────────────────────────
  EOT
}
