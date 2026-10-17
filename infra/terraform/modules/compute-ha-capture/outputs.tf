output "collector_instance_ids" {
  description = "EC2 instance IDs for Collector Leg A and Leg B"
  value       = aws_instance.collector[*].id
}

output "collector_private_ips" {
  description = "Private IP addresses for Collector Leg A and Leg B"
  value       = aws_instance.collector[*].private_ip
}

output "collector_elastic_ips" {
  description = "Static Elastic IP addresses for Collector Leg A and Leg B"
  value       = aws_eip.collector[*].public_ip
}

output "postgres_private_ip" {
  description = "Private IP of PostgreSQL catalog primary (empty if enable_data_plane = false)"
  value       = var.enable_data_plane ? aws_instance.postgres_catalog[0].private_ip : ""
}

output "redpanda_private_ips" {
  description = "Private IPs of the Redpanda Raft cluster nodes"
  value       = aws_instance.redpanda[*].private_ip
}

output "clickhouse_private_ips" {
  description = "Private IPs of the ClickHouse Replicated hot-store replicas"
  value       = aws_instance.clickhouse[*].private_ip
}
