# Incoming transactions. Its stream feeds the detector and the archiver.
resource "aws_dynamodb_table" "transactions" {
  name             = "${local.prefix}-transactions"
  billing_mode     = var.dynamodb_billing_mode
  read_capacity    = local.provisioned ? var.provisioned_capacity.transactions.read : null
  write_capacity   = local.provisioned ? var.provisioned_capacity.transactions.write : null
  hash_key         = "txn_id"
  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  attribute {
    name = "txn_id"
    type = "S"
  }

  # The table is a short-lived buffer; S3 holds the permanent copy. TTL deletes are free.
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Per-account sliding-window history used by the stateful rules.
resource "aws_dynamodb_table" "account_state" {
  name           = "${local.prefix}-account-state"
  billing_mode   = var.dynamodb_billing_mode
  read_capacity  = local.provisioned ? var.provisioned_capacity.account_state.read : null
  write_capacity = local.provisioned ? var.provisioned_capacity.account_state.write : null
  hash_key       = "account_id"

  attribute {
    name = "account_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Alerts, keyed so an investigator can query every alert for one account in time order.
resource "aws_dynamodb_table" "alerts" {
  name             = "${local.prefix}-alerts"
  billing_mode     = var.dynamodb_billing_mode
  read_capacity    = local.provisioned ? var.provisioned_capacity.alerts.read : null
  write_capacity   = local.provisioned ? var.provisioned_capacity.alerts.write : null
  hash_key         = "account_id"
  range_key        = "alert_key"
  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  attribute {
    name = "account_id"
    type = "S"
  }

  attribute {
    name = "alert_key"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}
