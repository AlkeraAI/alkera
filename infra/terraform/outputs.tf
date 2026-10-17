output "localstack_mode_enabled" {
  description = "Whether this deployment is targeting LocalStack (http://localhost:4566)"
  value       = var.use_localstack
}

output "polymarket_enabled" {
  description = "Whether Polymarket resources are enabled (false = Kalshi-only active focus)"
  value       = var.enable_polymarket
}

output "s3_lakehouse_bucket_name" {
  description = "S3 bucket storing ZSTD-9 Parquet partitions and ClickHouse tiered segments"
  value       = module.storage_lakehouse.s3_bucket_name
}

output "kalshi_secrets_manager_arns" {
  description = "AWS Secrets Manager ARNs storing KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PEM"
  value       = module.storage_lakehouse.kalshi_secret_arns
}

output "kalshi_ull_execution_node" {
  description = "Kalshi ULL execution node details in us-east-2 (use2-az1)"
  value = {
    instance_id = module.kalshi_ull_ohio.instance_id
    private_ip  = module.kalshi_ull_ohio.private_ip
    elastic_ip  = module.kalshi_ull_ohio.elastic_ip
  }
}

output "kalshi_ha_collectors" {
  description = "Kalshi Active-Active Collector Legs A (use2-az1) & B (use2-az2) in us-east-2"
  value = {
    instance_ids = module.kalshi_ha_capture.collector_instance_ids
    private_ips  = module.kalshi_ha_capture.collector_private_ips
    elastic_ips  = module.kalshi_ha_capture.collector_elastic_ips
  }
}

output "data_plane_endpoints" {
  description = "Internal endpoints for PostgreSQL Catalog, Redpanda, and ClickHouse in us-east-2"
  value = {
    postgres_catalog_ip = module.kalshi_ha_capture.postgres_private_ip
    redpanda_ips        = module.kalshi_ha_capture.redpanda_private_ips
    clickhouse_ips      = module.kalshi_ha_capture.clickhouse_private_ips
  }
}

output "grafana_observability_url" {
  description = "Unified Grafana dashboard URL"
  value       = module.observability.grafana_url
}
