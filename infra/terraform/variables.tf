variable "project_name" {
  description = "Project identifier prefix"
  type        = string
  default     = "bond"
}

variable "environment" {
  description = "Deployment environment (localstack, dev, staging, prod)"
  type        = string
  default     = "localstack"
}

variable "use_localstack" {
  description = "Route all AWS provider calls to LocalStack (http://localhost:4566) for local infrastructure and secret/S3 testing"
  type        = bool
  default     = true
}

variable "localstack_endpoint" {
  description = "LocalStack edge endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

variable "enable_polymarket" {
  description = "Enable Polymarket European regions (eu-west-2 London / eu-west-1 Dublin). Disabled by default to focus strictly on Kalshi (us-east-2)."
  type        = bool
  default     = false
}

variable "s3_lakehouse_bucket_name" {
  description = "S3 bucket name for ZSTD-9 Parquet lakehouse and ClickHouse cold tiering"
  type        = string
  default     = "bond-localstack-kalshi-lakehouse"
}

variable "kalshi_api_key_id" {
  description = "Kalshi Ed25519 API Key ID (KALSHI_API_KEY_ID) for WebSocket v2 and REST v2 authentication"
  type        = string
  default     = "kalshi-localstack-ed25519-key-id"
  sensitive   = true
}

variable "kalshi_private_key_pem" {
  description = "Kalshi Ed25519 Private Key PEM (KALSHI_PRIVATE_KEY_PEM) for signing timestamp_ms + METHOD + path"
  type        = string
  default     = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIB9pZGVudGljYWwtbG9jYWxzdGFjay1lZDI1NTE5LWtleQ==\n-----END PRIVATE KEY-----"
  sensitive   = true
}

variable "use2_physical_az_ids" {
  description = "Ordered physical AWS Availability Zone IDs in us-east-2 (Ohio) co-located with Kalshi ALB"
  type        = list(string)
  default     = ["use2-az1", "use2-az2", "use2-az3"]
}

variable "euw2_physical_az_ids" {
  description = "Ordered physical AWS Availability Zone IDs in eu-west-2 (London) when enable_polymarket = true"
  type        = list(string)
  default     = ["euw2-az2", "euw2-az1"]
}

variable "euw1_physical_az_ids" {
  description = "Ordered physical AWS Availability Zone IDs in eu-west-1 (Dublin) when enable_polymarket = true"
  type        = list(string)
  default     = ["euw1-az1", "euw1-az2"]
}

variable "ami_id_us_east_2" {
  description = "Ubuntu 24.04 LTS x86_64 AMI ID in us-east-2 (Ohio)"
  type        = string
  default     = "ami-0ea3c35c5c3284d82"
}

variable "ami_id_eu_west_2" {
  description = "Ubuntu 24.04 LTS x86_64 AMI ID in eu-west-2 (London)"
  type        = string
  default     = "ami-0e8d228ad90af673b"
}

variable "ami_id_eu_west_1" {
  description = "Ubuntu 24.04 LTS x86_64 AMI ID in eu-west-1 (Dublin)"
  type        = string
  default     = "ami-0e9085e60087ce171"
}

variable "ull_instance_type" {
  description = "EC2 instance type for core-pinned ULL execution nodes"
  type        = string
  default     = "c7i.4xlarge"
}

variable "collector_instance_type" {
  description = "EC2 instance type for Active-Active WebSocket collector legs"
  type        = string
  default     = "c7i.2xlarge"
}

variable "data_node_instance_type" {
  description = "EC2 instance type for Redpanda, ClickHouse, and PostgreSQL HA nodes"
  type        = string
  default     = "r7i.2xlarge"
}

variable "admin_ingress_cidrs" {
  description = "Authorized operator CIDRs allowed to access Grafana UI (:3000)"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}
