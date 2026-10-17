variable "project_name" {
  description = "Project identifier prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "region" {
  description = "AWS region name"
  type        = string
}

variable "region_label" {
  description = "Short region label (e.g., use2-ohio)"
  type        = string
}

variable "venue" {
  description = "Target venue (kalshi)"
  type        = string
  default     = "kalshi"
}

variable "ami_id" {
  description = "Ubuntu 24.04 LTS x86_64 AMI ID"
  type        = string
}

variable "collector_instance_type" {
  description = "Instance type for Active-Active Collector Legs A & B"
  type        = string
  default     = "c7i.2xlarge"
}

variable "data_node_instance_type" {
  description = "Instance type for Redpanda, ClickHouse, and PostgreSQL HA nodes"
  type        = string
  default     = "r7i.2xlarge"
}

variable "leg_a_subnet_id" {
  description = "Subnet ID pinned to Physical AZ 1 (e.g. use2-az1) for Collector Leg A"
  type        = string
}

variable "leg_b_subnet_id" {
  description = "Subnet ID pinned to Physical AZ 2 (e.g. use2-az2) for Collector Leg B"
  type        = string
}

variable "data_plane_subnet_ids" {
  description = "Ordered list of private subnet IDs across Physical AZs for Redpanda & ClickHouse"
  type        = list(string)
}

variable "collector_security_group_id" {
  description = "Security group ID for HA collectors"
  type        = string
}

variable "data_plane_security_group_id" {
  description = "Security group ID for Redpanda, ClickHouse, PostgreSQL, and Merger"
  type        = string
}

variable "iam_instance_profile" {
  description = "IAM instance profile name for collectors and data nodes"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 Parquet lakehouse bucket name"
  type        = string
}

variable "secrets_prefix" {
  description = "AWS Secrets Manager prefix for Kalshi Ed25519 keys"
  type        = string
}

variable "enable_data_plane" {
  description = "Whether to provision Redpanda, ClickHouse, and PostgreSQL HA nodes in this region"
  type        = bool
  default     = true
}

variable "wal_volume_size_gb" {
  description = "Size of dedicated gp3/NVMe Write-Ahead Log (WAL) EBS volume per collector leg"
  type        = number
  default     = 500
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default     = {}
}
