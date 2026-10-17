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
  description = "Short region label (e.g., use2-ohio, euw1-dublin, euw2-london)"
  type        = string
}

variable "venue_role" {
  description = "Execution role on this node (kalshi-primary, polymarket-active, polymarket-kyb-standby)"
  type        = string
}

variable "ami_id" {
  description = "Ubuntu 24.04 LTS x86_64 AMI ID for the region"
  type        = string
}

variable "instance_type" {
  description = "Compute-optimized Intel Sapphire Rapids instance with hardware SHA-NI & AVX-512"
  type        = string
  default     = "c7i.4xlarge"
}

variable "subnet_id" {
  description = "Subnet ID pinned to the target physical AZ ID (e.g. use2-az1, euw1-az1, euw2-az2)"
  type        = string
}

variable "security_group_id" {
  description = "Security group ID for the ULL execution node"
  type        = string
}

variable "iam_instance_profile" {
  description = "IAM instance profile name granting least-privilege Secrets Manager & SSM access"
  type        = string
}

variable "isolated_cores" {
  description = "Kernel isolcpus / nohz_full / rcu_nocbs CPU core range"
  type        = string
  default     = "2-7"
}

variable "peer_bridge_ip" {
  description = "Private IP of the transatlantic peer ULL node over VPC peering"
  type        = string
  default     = "127.0.0.1"
}

variable "secrets_prefix" {
  description = "AWS Secrets Manager prefix for venue keys"
  type        = string
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default     = {}
}
