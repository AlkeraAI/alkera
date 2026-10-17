#!/usr/bin/env bash
set -euo pipefail

export AWS_DEFAULT_REGION="us-east-2"

# 1. Provision S3 buckets for Parquet cold lakehouse, ClickHouse S3 disk tiering, and Redpanda tiered storage
awslocal s3 mb s3://alkera-parquet-lakehouse --region us-east-2 || true
awslocal s3 mb s3://alkera-clickhouse-cold --region us-east-2 || true
awslocal s3 mb s3://alkera-redpanda-tiered --region us-east-2 || true

# 2. Provision SSM Parameter Store runtime feature flags (Kalshi enabled, Polymarket disabled)
awslocal ssm put-parameter \
  --name "/alkera/config/kalshi_enabled" \
  --type "String" \
  --value "true" \
  --overwrite

awslocal ssm put-parameter \
  --name "/alkera/config/polymarket_enabled" \
  --type "String" \
  --value "false" \
  --overwrite

# 3. Provision AWS Secrets Manager secret slots for Kalshi Collector Legs A & B
awslocal secretsmanager create-secret \
  --name "alkera/kalshi/collector-leg-a" \
  --description "Kalshi Ed25519 API key and PEM for Collector Leg A (us-east-2a)" \
  --secret-string '{"KALSHI_API_KEY_ID":"replace-with-kalshi-key-id","KALSHI_PRIVATE_KEY_PEM":""}' || true

awslocal secretsmanager create-secret \
  --name "alkera/kalshi/collector-leg-b" \
  --description "Kalshi Ed25519 API key and PEM for Collector Leg B (us-east-2b)" \
  --secret-string '{"KALSHI_API_KEY_ID":"replace-with-kalshi-key-id","KALSHI_PRIVATE_KEY_PEM":""}' || true
