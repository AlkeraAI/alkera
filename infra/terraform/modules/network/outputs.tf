output "vpc_id" {
  description = "ID of the regional VPC"
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "CIDR block of the regional VPC"
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids_by_az_id" {
  description = "Map of physical AZ ID (e.g. use2-az1) to public subnet ID"
  value = {
    for idx, az_id in var.physical_az_ids :
    az_id => aws_subnet.public[idx].id
  }
}

output "private_subnet_ids_by_az_id" {
  description = "Map of physical AZ ID (e.g. use2-az1) to private subnet ID"
  value = {
    for idx, az_id in var.physical_az_ids :
    az_id => aws_subnet.private[idx].id
  }
}

output "public_route_table_id" {
  description = "Route table ID for public subnets (used for inter-region VPC peering routes)"
  value       = aws_route_table.public.id
}

output "private_route_table_id" {
  description = "Route table ID for private subnets (used for inter-region VPC peering routes)"
  value       = aws_route_table.private.id
}

output "ull_security_group_id" {
  description = "Security group ID for ULL execution nodes"
  value       = aws_security_group.ull_execution.id
}

output "collector_security_group_id" {
  description = "Security group ID for HA collectors"
  value       = aws_security_group.ha_collector.id
}

output "data_plane_security_group_id" {
  description = "Security group ID for Redpanda, ClickHouse, PostgreSQL, and Merger"
  value       = aws_security_group.data_plane.id
}

output "observability_security_group_id" {
  description = "Security group ID for Grafana and Prometheus"
  value       = aws_security_group.observability.id
}
