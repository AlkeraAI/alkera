# =============================================================================
# LocalStack Configuration (Default for Local Dev & CI Integration Testing)
# Usage: terraform apply -var-file=localstack.tfvars
# =============================================================================
project_name             = "bond"
environment              = "localstack"
use_localstack           = true
localstack_endpoint      = "http://localhost:4566"
enable_polymarket        = false
s3_lakehouse_bucket_name = "bond-localstack-kalshi-lakehouse"

# Kalshi Ed25519 Test Credentials (Seeded into LocalStack Secrets Manager)
kalshi_api_key_id      = "00000000-1111-2222-3333-444444444444"
kalshi_private_key_pem = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIB9pZGVudGljYWwtbG9jYWxzdGFjay1lZDI1NTE5LWtleQ==\n-----END PRIVATE KEY-----"
