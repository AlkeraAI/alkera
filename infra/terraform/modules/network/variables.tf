variable "project_name" {
  description = "Project identifier prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "region_label" {
  description = "Short region label (e.g., use2-ohio, euw2-london, euw1-dublin)"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the regional VPC"
  type        = string
}

variable "physical_az_ids" {
  description = "Ordered list of physical AWS Availability Zone IDs (e.g., [\"use2-az1\", \"use2-az2\", \"use2-az3\"])"
  type        = list(string)
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDRs corresponding 1:1 with physical_az_ids"
  type        = list(string)
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDRs corresponding 1:1 with physical_az_ids"
  type        = list(string)
}

variable "peer_vpc_cidrs" {
  description = "CIDR blocks of peered remote VPCs allowed for cross-region bridge, collector, and telemetry traffic"
  type        = list(string)
  default     = []
}

variable "admin_ingress_cidrs" {
  description = "Trusted operator CIDR blocks allowed to reach Grafana/SSH"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default     = {}
}
