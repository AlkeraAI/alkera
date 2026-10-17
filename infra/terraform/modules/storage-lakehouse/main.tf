locals {
  name_prefix    = "${var.project_name}-${var.environment}"
  secrets_prefix = "/${var.project_name}/${var.environment}"
}

# 1. Customer-Managed KMS Key for S3 Parquet Lakehouse & AWS Secrets Manager
resource "aws_kms_key" "bond_master" {
  description             = "Project Bond KMS key for S3 Parquet Lakehouse and Kalshi Ed25519 Secrets"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-kms-key"
  })
}

resource "aws_kms_alias" "bond_master" {
  name          = "alias/${local.name_prefix}-master"
  target_key_id = aws_kms_key.bond_master.key_id
}

# 2. S3 Lakehouse Bucket (ZSTD-9 Partitioned Parquet + ClickHouse s3_cold Tier)
resource "aws_s3_bucket" "lakehouse" {
  bucket = var.s3_bucket_name

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-parquet-lakehouse"
  })
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.bond_master.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }

  rule {
    id     = "transition-historical-parquet-to-glacier-ir"
    status = "Enabled"

    filter {
      prefix = "v1/canonical/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

# 3. Kalshi Ed25519 Secrets in AWS Secrets Manager (Active Focus)
#    Stores KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PEM for Kalshi WebSockets & REST v2
locals {
  kalshi_secrets = {
    "kalshi/ws_and_rest_default"      = "Default Kalshi Ed25519 credentials (KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PEM)"
    "kalshi/capture_leg_a_ed25519"    = "Kalshi HA Collector Leg A Ed25519 key pair (wss://external-api-ws.kalshi.com)"
    "kalshi/capture_leg_b_ed25519"    = "Kalshi HA Collector Leg B Ed25519 key pair (wss://external-api-ws.kalshi.com)"
    "kalshi/ull_exec_primary_ed25519" = "Kalshi ULL Execution Primary Ed25519 key pair (256ms pre-signed ring)"
    "kalshi/ull_exec_standby_ed25519" = "Kalshi ULL Execution Standby Ed25519 key pair (zero-downtime key rotation)"
  }

  polymarket_secrets = var.enable_polymarket ? {
    "polymarket/hot_signer_secp256k1" = "Polymarket hot operator EOA secp256k1 key (signatureType=1 proxy delegate)"
    "polymarket/l2_clob_hmac"         = "Polymarket L2 CLOB API key, secret, and passphrase"
  } : {}

  all_secrets = merge(local.kalshi_secrets, local.polymarket_secrets)
}

resource "aws_secretsmanager_secret" "venue_credentials" {
  for_each = local.all_secrets

  name                    = "${local.secrets_prefix}/${each.key}"
  description             = each.value
  kms_key_id              = aws_kms_key.bond_master.arn
  recovery_window_in_days = 0

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-${replace(each.key, "/", "-")}"
  })
}

resource "aws_secretsmanager_secret_version" "kalshi_default_seed" {
  for_each = local.kalshi_secrets

  secret_id = aws_secretsmanager_secret.venue_credentials[each.key].id
  secret_string = jsonencode({
    KALSHI_API_KEY_ID      = var.kalshi_api_key_id
    KALSHI_PRIVATE_KEY_PEM = var.kalshi_private_key_pem
    KALSHI_WS_URL          = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    KALSHI_REST_URL        = "https://external-api.kalshi.com/trade-api/v2"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# 4. Least-Privilege IAM Roles & Instance Profiles for Collectors and ULL Nodes
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bond_node" {
  name               = "${local.name_prefix}-node-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = var.tags
}

data "aws_iam_policy_document" "bond_node_permissions" {
  statement {
    sid    = "LakehouseS3ReadWrite"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload"
    ]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*"
    ]
  }

  statement {
    sid    = "ReadVenueSecrets"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = [for s in aws_secretsmanager_secret.venue_credentials : s.arn]
  }

  statement {
    sid    = "KmsDecryptEncrypt"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey"
    ]
    resources = [aws_kms_key.bond_master.arn]
  }
}

resource "aws_iam_role_policy" "bond_node" {
  name   = "${local.name_prefix}-node-policy"
  role   = aws_iam_role.bond_node.id
  policy = data.aws_iam_policy_document.bond_node_permissions.json
}

resource "aws_iam_instance_profile" "bond_node" {
  name = "${local.name_prefix}-node-profile"
  role = aws_iam_role.bond_node.name

  tags = var.tags
}
