locals {
  name_prefix = "${var.project_name}-${var.environment}-${var.region_label}-ull"
}

resource "aws_placement_group" "ull_cluster" {
  name     = "${local.name_prefix}-cluster-pg"
  strategy = "cluster"

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-cluster-pg"
  })
}

resource "aws_instance" "ull_node" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = var.iam_instance_profile
  placement_group        = aws_placement_group.ull_cluster.id
  ebs_optimized          = true
  monitoring             = true

  user_data = templatefile("${path.module}/../../cloud-init/ull-node.yaml.tftpl", {
    environment    = var.environment
    region         = var.region
    venue_role     = var.venue_role
    peer_bridge_ip = var.peer_bridge_ip
    secrets_prefix = var.secrets_prefix
    isolated_cores = var.isolated_cores
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "enabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 120
    iops                  = 6000
    throughput            = 500
    encrypted             = true
    delete_on_termination = true
  }

  tags = merge(var.tags, {
    Name      = "${local.name_prefix}-${var.venue_role}"
    Role      = "ull-execution"
    VenueRole = var.venue_role
  })
}

resource "aws_eip" "ull_static" {
  domain   = "vpc"
  instance = aws_instance.ull_node.id

  tags = merge(var.tags, {
    Name      = "${local.name_prefix}-${var.venue_role}-eip"
    VenueRole = var.venue_role
  })
}
