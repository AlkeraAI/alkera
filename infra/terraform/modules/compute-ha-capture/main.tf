locals {
  name_prefix        = "${var.project_name}-${var.environment}-${var.region_label}"
  collector_legs     = ["a", "b"]
  collector_subnets  = [var.leg_a_subnet_id, var.leg_b_subnet_id]
  redpanda_node_cnt  = var.enable_data_plane ? min(3, length(var.data_plane_subnet_ids)) : 0
  clickhouse_cnt     = var.enable_data_plane ? min(2, length(var.data_plane_subnet_ids)) : 0
  redpanda_broker_ls = var.enable_data_plane ? join(",", [for i in range(local.redpanda_node_cnt) : "10.20.${11 + i}.50:9092"]) : "localhost:9092"
}

resource "aws_placement_group" "ha_spread" {
  name         = "${local.name_prefix}-${var.venue}-spread-pg"
  strategy     = "spread"
  spread_level = "rack"

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-${var.venue}-spread-pg"
  })
}

# Active-Active Collector Legs A & B in distinct Physical Availability Zones
resource "aws_instance" "collector" {
  count = 2

  ami                    = var.ami_id
  instance_type          = var.collector_instance_type
  subnet_id              = local.collector_subnets[count.index]
  vpc_security_group_ids = [var.collector_security_group_id]
  iam_instance_profile   = var.iam_instance_profile
  placement_group        = aws_placement_group.ha_spread.id
  ebs_optimized          = true
  monitoring             = true

  user_data = templatefile("${path.module}/../../cloud-init/ha-collector.yaml.tftpl", {
    environment      = var.environment
    region           = var.region
    venue            = var.venue
    collector_leg    = local.collector_legs[count.index]
    redpanda_brokers = local.redpanda_broker_ls
    postgres_host    = var.enable_data_plane ? aws_instance.postgres_catalog[0].private_ip : "127.0.0.1"
    s3_bucket_name   = var.s3_bucket_name
    secrets_prefix   = var.secrets_prefix
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "enabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 100
    iops                  = 3000
    throughput            = 250
    encrypted             = true
    delete_on_termination = true
  }

  ebs_block_device {
    device_name           = "/dev/sdf"
    volume_type           = "gp3"
    volume_size           = var.wal_volume_size_gb
    iops                  = 12000
    throughput            = 750
    encrypted             = true
    delete_on_termination = false
  }

  tags = merge(var.tags, {
    Name         = "${local.name_prefix}-${var.venue}-collector-leg-${local.collector_legs[count.index]}"
    Role         = "alkera-collector"
    Venue        = var.venue
    CollectorLeg = local.collector_legs[count.index]
  })
}

resource "aws_eip" "collector" {
  count = 2

  domain   = "vpc"
  instance = aws_instance.collector[count.index].id

  tags = merge(var.tags, {
    Name         = "${local.name_prefix}-${var.venue}-collector-leg-${local.collector_legs[count.index]}-eip"
    Venue        = var.venue
    CollectorLeg = local.collector_legs[count.index]
  })
}

# PostgreSQL HA Relational Catalog & Surrogate ID (instrument_iid) Coordinator
resource "aws_instance" "postgres_catalog" {
  count = var.enable_data_plane ? 1 : 0

  ami                    = var.ami_id
  instance_type          = var.data_node_instance_type
  subnet_id              = var.data_plane_subnet_ids[0]
  vpc_security_group_ids = [var.data_plane_security_group_id]
  iam_instance_profile   = var.iam_instance_profile
  ebs_optimized          = true
  monitoring             = true

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 250
    iops                  = 6000
    throughput            = 500
    encrypted             = true
    delete_on_termination = false
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-postgres-catalog-primary"
    Role = "postgres-catalog"
  })
}

# 3-AZ Redpanda Raft Cluster + Stream Merger (alkera-merger)
resource "aws_instance" "redpanda" {
  count = local.redpanda_node_cnt

  ami                    = var.ami_id
  instance_type          = var.data_node_instance_type
  subnet_id              = var.data_plane_subnet_ids[count.index]
  vpc_security_group_ids = [var.data_plane_security_group_id]
  iam_instance_profile   = var.iam_instance_profile
  ebs_optimized          = true
  monitoring             = true

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 500
    iops                  = 12000
    throughput            = 750
    encrypted             = true
    delete_on_termination = false
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-redpanda-broker-${count.index}"
    Role = "redpanda-merger"
  })
}

# 2-AZ ClickHouse ReplicatedReplacingMergeTree Hot Analytical Cluster
resource "aws_instance" "clickhouse" {
  count = local.clickhouse_cnt

  ami                    = var.ami_id
  instance_type          = var.data_node_instance_type
  subnet_id              = var.data_plane_subnet_ids[count.index]
  vpc_security_group_ids = [var.data_plane_security_group_id]
  iam_instance_profile   = var.iam_instance_profile
  ebs_optimized          = true
  monitoring             = true

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 1000
    iops                  = 16000
    throughput            = 1000
    encrypted             = true
    delete_on_termination = false
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-clickhouse-replica-${count.index}"
    Role = "clickhouse-hot-store"
  })
}
