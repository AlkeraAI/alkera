output "kms_key_arn" {
  description = "ARN of the customer-managed KMS encryption key"
  value       = aws_kms_key.bond_master.arn
}

output "s3_bucket_name" {
  description = "Name of the S3 Parquet lakehouse bucket"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 Parquet lakehouse bucket"
  value       = aws_s3_bucket.lakehouse.arn
}

output "secrets_prefix" {
  description = "Secrets Manager path prefix for venue credentials"
  value       = local.secrets_prefix
}

output "kalshi_secret_arns" {
  description = "Map of Kalshi secret keys to their AWS Secrets Manager ARNs"
  value = {
    for k, s in aws_secretsmanager_secret.venue_credentials :
    k => s.arn
  }
}

output "iam_instance_profile_name" {
  description = "IAM instance profile name attached to ULL and HA Collector EC2 instances"
  value       = aws_iam_instance_profile.bond_node.name
}
