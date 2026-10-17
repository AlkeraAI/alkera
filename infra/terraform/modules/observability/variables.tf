variable "project_name" {
  description = "Project identifier prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "region_label" {
  description = "Short region label (e.g., use2-ohio)"
  type        = string
}

variable "ami_id" {
  description = "Ubuntu 24.04 LTS x86_64 AMI ID"
  type        = string
}

variable "instance_type" {
  description = "Instance type for the Unified Grafana + Prometheus + Alertmanager server"
  type        = string
  default     = "m7i.xlarge"
}

variable "subnet_id" {
  description = "Subnet ID (pinned to use2-az3)"
  type        = string
}

variable "security_group_id" {
  description = "Security group ID for observability host"
  type        = string
}

variable "iam_instance_profile" {
  description = "IAM instance profile name"
  type        = string
}

variable "ull_scrape_targets" {
  description = "List of host:port targets for ULL execution engines (:9102)"
  type        = list(string)
}

variable "collector_scrape_targets" {
  description = "List of host:port targets for HA collectors (:9108)"
  type        = list(string)
}

variable "data_plane_scrape_targets" {
  description = "List of host:port targets for Redpanda, ClickHouse, and Merger metrics"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default     = {}
}
