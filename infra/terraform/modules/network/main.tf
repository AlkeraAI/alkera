locals {
  name_prefix        = "${var.project_name}-${var.environment}-${var.region_label}"
  all_internal_cidrs = concat([var.vpc_cidr], var.peer_vpc_cidrs)
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-igw"
  })
}

resource "aws_subnet" "public" {
  count = length(var.physical_az_ids)

  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone_id    = var.physical_az_ids[count.index]
  map_public_ip_on_launch = false

  tags = merge(var.tags, {
    Name         = "${local.name_prefix}-public-${var.physical_az_ids[count.index]}"
    PhysicalAzId = var.physical_az_ids[count.index]
    Tier         = "public"
  })
}

resource "aws_subnet" "private" {
  count = length(var.physical_az_ids)

  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.private_subnet_cidrs[count.index]
  availability_zone_id    = var.physical_az_ids[count.index]
  map_public_ip_on_launch = false

  tags = merge(var.tags, {
    Name         = "${local.name_prefix}-private-${var.physical_az_ids[count.index]}"
    PhysicalAzId = var.physical_az_ids[count.index]
    Tier         = "private"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-public-rt"
  })
}

resource "aws_route_table_association" "public" {
  count = length(var.physical_az_ids)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-private-rt"
  })
}

resource "aws_route_table_association" "private" {
  count = length(var.physical_az_ids)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# 1. Security Group for Ultra-Low-Latency (ULL) Execution Nodes
resource "aws_security_group" "ull_execution" {
  name        = "${local.name_prefix}-ull-exec-sg"
  description = "Security group for ULL execution nodes (cross-region UDP/TCP BBR bridge + Prometheus)"
  vpc_id      = aws_vpc.this.id

  # Cross-region arbitrage bridge: dual ECMP UDP sockets (19001-19002) + London->Dublin fast-forward (19003)
  ingress {
    description = "ULL Cross-Region ECMP UDP Trigger Bridge"
    from_port   = 19001
    to_port     = 19003
    protocol    = "udp"
    cidr_blocks = local.all_internal_cidrs
  }

  # Cross-region arbitrage bridge: persistent TCP_NODELAY BBR control stream (19000)
  ingress {
    description = "ULL Cross-Region TCP BBR Control Bridge"
    from_port   = 19000
    to_port     = 19000
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  # Lock-free Prometheus metrics exporter (:9102/metrics)
  ingress {
    description = "Prometheus ULL hot-path metrics scrape"
    from_port   = 9102
    to_port     = 9102
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  egress {
    description = "Unrestricted outbound TLS 1.3 to Kalshi ALB, Polymarket CLOB, and peered VPCs"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-ull-exec-sg"
  })
}

# 2. Security Group for Active-Active Market Data Collectors (Leg A & Leg B)
resource "aws_security_group" "ha_collector" {
  name        = "${local.name_prefix}-ha-collector-sg"
  description = "Security group for Active-Active Kalshi and Polymarket WebSocket collectors"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "Prometheus collector telemetry scrape (:9108)"
    from_port   = 9108
    to_port     = 9108
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  egress {
    description = "Outbound WebSocket/REST to venues and rkyv streams to Redpanda/PostgreSQL"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-ha-collector-sg"
  })
}

# 3. Security Group for HA Data Plane (Redpanda, ClickHouse, PostgreSQL, Merger)
resource "aws_security_group" "data_plane" {
  name        = "${local.name_prefix}-data-plane-sg"
  description = "Security group for Redpanda, ClickHouse Keeper/Replicas, PostgreSQL Catalog, and Merger"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "Redpanda Kafka API & Raft RPC"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  ingress {
    description = "Redpanda Internal RPC & Admin metrics"
    from_port   = 9644
    to_port     = 9644
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  ingress {
    description = "ClickHouse Native TCP & Inter-Replica Replication"
    from_port   = 9000
    to_port     = 9009
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  ingress {
    description = "ClickHouse HTTP SQL & Keeper Raft"
    from_port   = 8123
    to_port     = 8123
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  ingress {
    description = "ClickHouse Keeper Coordination"
    from_port   = 9181
    to_port     = 9285
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  ingress {
    description = "PostgreSQL Relational Catalog & LISTEN/NOTIFY"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  ingress {
    description = "Merger & Reconciler Prometheus metrics"
    from_port   = 9108
    to_port     = 9112
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  egress {
    description = "Allow outbound to S3 lakehouse and peer nodes"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-data-plane-sg"
  })
}

# 4. Security Group for Unified Observability (Grafana + Prometheus + Alertmanager)
resource "aws_security_group" "observability" {
  name        = "${local.name_prefix}-observability-sg"
  description = "Security group for Grafana unified observability, Prometheus, and Alertmanager"
  vpc_id      = aws_vpc.this.id

  dynamic "ingress" {
    for_each = length(var.admin_ingress_cidrs) > 0 ? [1] : []
    content {
      description = "Grafana HTTPS/HTTP UI access for authorized operators"
      from_port   = 3000
      to_port     = 3000
      protocol    = "tcp"
      cidr_blocks = var.admin_ingress_cidrs
    }
  }

  ingress {
    description = "Internal VPC access to Grafana, Prometheus (9090), and Alertmanager (9093)"
    from_port   = 3000
    to_port     = 9093
    protocol    = "tcp"
    cidr_blocks = local.all_internal_cidrs
  }

  egress {
    description = "Allow scraping all regional collectors, ULL nodes, and data plane services"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${local.name_prefix}-observability-sg"
  })
}
