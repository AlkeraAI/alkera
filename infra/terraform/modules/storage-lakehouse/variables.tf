variable "project_name" {
  description = "Project identifier prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment (localstack, dev, staging, prod)"
  type        = string
}

variable "s3_bucket_name" {
  description = "Globally unique S3 bucket name for the ZSTD-9 Parquet Lakehouse and ClickHouse cold tier"
  type        = string
}

variable "enable_polymarket" {
  description = "Whether to provision Polymarket secret containers (disabled by default; Kalshi-only active focus)"
  type        = bool
  default     = false
}

variable "kalshi_api_key_id" {
  description = "Default/seed KALSHI_API_KEY_ID stored in Secrets Manager (populated in LocalStack or overridden out-of-band in prod)"
  type        = string
  default     = "kalshi-ed25519-key-id-placeholder"
  sensitive   = true
}

variable "kalshi_private_key_pem" {
  description = "Default/seed KALSHI_PRIVATE_KEY_PEM (Ed25519 PKCS#8 PEM) stored in Secrets Manager for Kalshi WebSockets & REST"
  type        = string
  default     = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIB9pZGVudGljYWwtbG9jYWxzdGFjay1lZDI1NTE5LWtleQ==\n-----END PRIVATE KEY-----"
  sensitive   = true
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default     = {}
}
