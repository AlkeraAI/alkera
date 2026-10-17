locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    ActiveScope = var.enable_polymarket ? "kalshi-and-polymarket" : "kalshi-only-single-vpc"
  }

  use2_vpc_cidr = "10.20.0.0/16"
  euw2_vpc_cidr = "10.21.0.0/16"
  euw1_vpc_cidr = "10.22.0.0/16"
}

# =============================================================================
# 1. AWS PROVIDERS (Supports LocalStack http://localhost:4566 & Live AWS)
# =============================================================================

provider "aws" {
  alias                       = "us_east_2"
  region                      = "us-east-2"
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  s3_use_path_style           = var.use_localstack
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack

  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      ec2            = var.localstack_endpoint
      iam            = var.localstack_endpoint
      kms            = var.localstack_endpoint
      s3             = var.localstack_endpoint
      secretsmanager = var.localstack_endpoint
      ssm            = var.localstack_endpoint
      sts            = var.localstack_endpoint
    }
  }
}

provider "aws" {
  alias                       = "eu_west_2"
  region                      = "eu-west-2"
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  s3_use_path_style           = var.use_localstack
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack

  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      ec2            = var.localstack_endpoint
      iam            = var.localstack_endpoint
      kms            = var.localstack_endpoint
      s3             = var.localstack_endpoint
      secretsmanager = var.localstack_endpoint
      ssm            = var.localstack_endpoint
      sts            = var.localstack_endpoint
    }
  }
}

provider "aws" {
  alias                       = "eu_west_1"
  region                      = "eu-west-1"
  access_key                  = var.use_localstack ? "test" : null
  secret_key                  = var.use_localstack ? "test" : null
  s3_use_path_style           = var.use_localstack
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack

  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      ec2            = var.localstack_endpoint
      iam            = var.localstack_endpoint
      kms            = var.localstack_endpoint
      s3             = var.localstack_endpoint
      secretsmanager = var.localstack_endpoint
      ssm            = var.localstack_endpoint
      sts            = var.localstack_endpoint
    }
  }
}

# =============================================================================
# 2. STORAGE, KMS, SECRETS MANAGER (KALSHI ED25519 KEYS) & IAM PROFILES
# =============================================================================

module "storage_lakehouse" {
  source = "./modules/storage-lakehouse"
  providers = {
    aws = aws.us_east_2
  }

  project_name           = var.project_name
  environment            = var.environment
  s3_bucket_name         = var.s3_lakehouse_bucket_name
  enable_polymarket      = var.enable_polymarket
  kalshi_api_key_id      = var.kalshi_api_key_id
  kalshi_private_key_pem = var.kalshi_private_key_pem
  tags                   = local.common_tags
}

# =============================================================================
# 3. SINGLE REGIONAL VPC IN AWS us-east-2 (Ohio, 10.20.0.0/16)
#    When enable_polymarket = false (default), 100% of the stack (Kalshi Collectors
#    Leg A & B, Kalshi ULL Execution Engine, Redpanda, ClickHouse, PostgreSQL, and
#    Unified Grafana) runs inside this single VPC across use2-az1, use2-az2, use2-az3
#    with zero VPC peering hops.
# =============================================================================

module "network_use2_ohio" {
  source = "./modules/network"
  providers = {
    aws = aws.us_east_2
  }

  project_name         = var.project_name
  environment          = var.environment
  region_label         = "use2-ohio"
  vpc_cidr             = local.use2_vpc_cidr
  physical_az_ids      = var.use2_physical_az_ids
  public_subnet_cidrs  = ["10.20.1.0/24", "10.20.2.0/24", "10.20.3.0/24"]
  private_subnet_cidrs = ["10.20.11.0/24", "10.20.12.0/24", "10.20.13.0/24"]
  peer_vpc_cidrs       = var.enable_polymarket ? [local.euw2_vpc_cidr, local.euw1_vpc_cidr] : []
  admin_ingress_cidrs  = var.admin_ingress_cidrs
  tags                 = local.common_tags
}

module "kalshi_ha_capture" {
  source = "./modules/compute-ha-capture"
  providers = {
    aws = aws.us_east_2
  }

  project_name                 = var.project_name
  environment                  = var.environment
  region                       = "us-east-2"
  region_label                 = "use2-ohio"
  venue                        = "kalshi"
  ami_id                       = var.ami_id_us_east_2
  collector_instance_type      = var.collector_instance_type
  data_node_instance_type      = var.data_node_instance_type
  leg_a_subnet_id              = module.network_use2_ohio.public_subnet_ids_by_az_id[var.use2_physical_az_ids[0]]
  leg_b_subnet_id              = module.network_use2_ohio.public_subnet_ids_by_az_id[var.use2_physical_az_ids[1]]
  data_plane_subnet_ids        = values(module.network_use2_ohio.private_subnet_ids_by_az_id)
  collector_security_group_id  = module.network_use2_ohio.collector_security_group_id
  data_plane_security_group_id = module.network_use2_ohio.data_plane_security_group_id
  iam_instance_profile         = module.storage_lakehouse.iam_instance_profile_name
  s3_bucket_name               = module.storage_lakehouse.s3_bucket_name
  secrets_prefix               = module.storage_lakehouse.secrets_prefix
  enable_data_plane            = true
  tags                         = local.common_tags
}

module "kalshi_ull_ohio" {
  source = "./modules/compute-ull"
  providers = {
    aws = aws.us_east_2
  }

  project_name         = var.project_name
  environment          = var.environment
  region               = "us-east-2"
  region_label         = "use2-ohio"
  venue_role           = "kalshi-primary"
  ami_id               = var.ami_id_us_east_2
  instance_type        = var.ull_instance_type
  subnet_id            = module.network_use2_ohio.public_subnet_ids_by_az_id[var.use2_physical_az_ids[0]]
  security_group_id    = module.network_use2_ohio.ull_security_group_id
  iam_instance_profile = module.storage_lakehouse.iam_instance_profile_name
  secrets_prefix       = module.storage_lakehouse.secrets_prefix
  tags                 = local.common_tags
}

# =============================================================================
# 4. UNIFIED GRAFANA & PROMETHEUS OBSERVABILITY STACK (Inside Single us-east-2 VPC)
# =============================================================================

module "observability" {
  source = "./modules/observability"
  providers = {
    aws = aws.us_east_2
  }

  project_name              = var.project_name
  environment               = var.environment
  region_label              = "use2-ohio"
  ami_id                    = var.ami_id_us_east_2
  subnet_id                 = module.network_use2_ohio.public_subnet_ids_by_az_id[var.use2_physical_az_ids[2]]
  security_group_id         = module.network_use2_ohio.observability_security_group_id
  iam_instance_profile      = module.storage_lakehouse.iam_instance_profile_name
  ull_scrape_targets        = ["${module.kalshi_ull_ohio.private_ip}:9102"]
  collector_scrape_targets  = [for ip in module.kalshi_ha_capture.collector_private_ips : "${ip}:9108"]
  data_plane_scrape_targets = [for ip in module.kalshi_ha_capture.redpanda_private_ips : "${ip}:9644"]
  clickhouse_host           = module.kalshi_ha_capture.clickhouse_private_ips[0]
  postgres_host             = module.kalshi_ha_capture.postgres_private_ip
  tags                      = local.common_tags
}

# =============================================================================
# 5. OPTIONAL POLYMARKET EUROPEAN VPCs & CROSS-REGION PEERING
#    Disabled by default (enable_polymarket = false). Because AWS VPCs cannot span
#    across geographic regions, European VPCs and Inter-Region VPC Peering are only
#    created when enable_polymarket = true.
# =============================================================================

module "network_euw2_london" {
  count  = var.enable_polymarket ? 1 : 0
  source = "./modules/network"
  providers = {
    aws = aws.eu_west_2
  }

  project_name         = var.project_name
  environment          = var.environment
  region_label         = "euw2-london"
  vpc_cidr             = local.euw2_vpc_cidr
  physical_az_ids      = var.euw2_physical_az_ids
  public_subnet_cidrs  = ["10.21.1.0/24", "10.21.2.0/24"]
  private_subnet_cidrs = ["10.21.11.0/24", "10.21.12.0/24"]
  peer_vpc_cidrs       = [local.use2_vpc_cidr, local.euw1_vpc_cidr]
  admin_ingress_cidrs  = var.admin_ingress_cidrs
  tags                 = local.common_tags
}

module "network_euw1_dublin" {
  count  = var.enable_polymarket ? 1 : 0
  source = "./modules/network"
  providers = {
    aws = aws.eu_west_1
  }

  project_name         = var.project_name
  environment          = var.environment
  region_label         = "euw1-dublin"
  vpc_cidr             = local.euw1_vpc_cidr
  physical_az_ids      = var.euw1_physical_az_ids
  public_subnet_cidrs  = ["10.22.1.0/24", "10.22.2.0/24"]
  private_subnet_cidrs = ["10.22.11.0/24", "10.22.12.0/24"]
  peer_vpc_cidrs       = [local.use2_vpc_cidr, local.euw2_vpc_cidr]
  admin_ingress_cidrs  = var.admin_ingress_cidrs
  tags                 = local.common_tags
}

resource "aws_vpc_peering_connection" "use2_to_euw1" {
  count    = var.enable_polymarket ? 1 : 0
  provider = aws.us_east_2

  vpc_id      = module.network_use2_ohio.vpc_id
  peer_vpc_id = module.network_euw1_dublin[0].vpc_id
  peer_region = "eu-west-1"
  auto_accept = false

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-${var.environment}-peering-use2-euw1"
  })
}

resource "aws_vpc_peering_connection_accepter" "euw1_accept_use2" {
  count    = var.enable_polymarket ? 1 : 0
  provider = aws.eu_west_1

  vpc_peering_connection_id = aws_vpc_peering_connection.use2_to_euw1[0].id
  auto_accept               = true

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-${var.environment}-peering-use2-euw1-accepter"
  })
}

resource "aws_route" "use2_to_euw1_public" {
  count    = var.enable_polymarket ? 1 : 0
  provider = aws.us_east_2

  route_table_id            = module.network_use2_ohio.public_route_table_id
  destination_cidr_block    = local.euw1_vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.use2_to_euw1[0].id
}

resource "aws_route" "euw1_to_use2_public" {
  count    = var.enable_polymarket ? 1 : 0
  provider = aws.eu_west_1

  route_table_id            = module.network_euw1_dublin[0].public_route_table_id
  destination_cidr_block    = local.use2_vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.use2_to_euw1[0].id
}
