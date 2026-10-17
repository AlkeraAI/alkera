output "instance_id" {
  description = "EC2 instance ID of the ULL execution node"
  value       = aws_instance.ull_node.id
}

output "private_ip" {
  description = "Private IP address of the ULL execution node (used for VPC peering bridge)"
  value       = aws_instance.ull_node.private_ip
}

output "elastic_ip" {
  description = "Static public Elastic IP of the ULL execution node (used for exchange whitelisting)"
  value       = aws_eip.ull_static.public_ip
}

output "placement_group_id" {
  description = "Cluster placement group ID"
  value       = aws_placement_group.ull_cluster.id
}
