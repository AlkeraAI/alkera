locals {
  name_prefix = "${var.project_name}-${var.environment}-${var.region_label}-obs"
}

resource "aws_instance" "grafana_prometheus" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = var.iam_instance_profile
  ebs_optimized          = true
  monitoring             = true

  user_data = templatefile("${path.module}/../../cloud-init/observability.yaml.tftpl", {
    environment               = var.environment
    ull_scrape_targets        = var.ull_scrape_targets
    collector_scrape_targets  = var.collector_scrape_targets
    data_plane_scrape_targets = var.data_plane_scrape_targets
    clickhouse_host           = var.clickhouse_host
    postgres_host             = var.postgres_host
    master_dashboard_json     = file("${path.module}/dashboards/bond-unified-master.json")
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "enabled"
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
    Name = "${local.name_prefix}-grafana-prometheus"
    Role = "observability-grafana"
  })
}

resource "aws_eip" "grafana" {
  domain   = "vpc"
  instance = aws_instance.grafana_prometheus.id

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-grafana-eip"
  })
}
