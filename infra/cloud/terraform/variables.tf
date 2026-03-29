variable "hcloud_token" {
  description = "Hetzner Cloud API token (generate at console.hetzner.cloud → Security → API Tokens)"
  type        = string
  sensitive   = true
}

variable "ssh_public_key" {
  description = "SSH public key content (e.g. contents of ~/.ssh/id_ed25519.pub)"
  type        = string
}

variable "project_name" {
  description = "Prefix for all resource names"
  type        = string
  default     = "crypto-analytics"
}

variable "location" {
  description = "Hetzner datacenter location (nbg1=Nuremberg, fsn1=Falkenstein, hel1=Helsinki)"
  type        = string
  default     = "nbg1"
}

variable "domain" {
  description = "Your domain name for TLS (e.g. crypto.yourdomain.com). Point its A record at data-1's public IP after terraform apply."
  type        = string
}

variable "existing_ssh_key_name" {
  description = "Exact name of an already uploaded Hetzner Cloud SSH key"
  type        = string
}

