output "instance_id" {
  description = "Instance ID of the Unified Grafana + Prometheus server"
  value       = aws_instance.grafana_prometheus.id
}

output "private_ip" {
  description = "Private IP address of the Unified Grafana + Prometheus server"
  value       = aws_instance.grafana_prometheus.private_ip
}

output "public_ip" {
  description = "Public Elastic IP of the Unified Grafana server"
  value       = aws_eip.grafana.public_ip
}

output "grafana_url" {
  description = "URL of the Unified Grafana dashboard interface"
  value       = "http://${aws_eip.grafana.public_ip}:3000"
}
