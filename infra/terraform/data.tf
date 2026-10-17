# =============================================================================
# 1. LIVE AWS DATA SOURCES (Caller Identity & us-east-2 AZ ID Mapping)
#    Skipped automatically when use_localstack = true so offline plan works too
# =============================================================================

data "aws_caller_identity" "current" {
  count    = var.use_localstack ? 0 : 1
  provider = aws.us_east_2
}

data "aws_availability_zones" "use2" {
  count    = var.use_localstack ? 0 : 1
  provider = aws.us_east_2
  state    = "available"
}
