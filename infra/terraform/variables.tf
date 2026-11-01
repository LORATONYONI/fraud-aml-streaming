variable "project_name" {
  description = "Prefix for every resource name. Lowercase letters, digits and hyphens."
  type        = string
  default     = "fraud-aml"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "project_name must be 3-21 characters: lowercase letters, digits, hyphens."
  }
}

variable "aws_region" {
  description = "AWS region. us-east-1 has every service used here and the lowest prices."
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = "Email for fraud alerts, pipeline-health alarms and the cost budget. Empty disables all three."
  type        = string
  default     = ""
}

variable "dynamodb_billing_mode" {
  description = <<-EOT
    PROVISIONED stays inside DynamoDB's always-free 25 read + 25 write capacity units (and caps
    throughput at roughly 10 transactions/second). PAY_PER_REQUEST costs nothing while idle but
    charges per request (fractions of a cent per demo run).
  EOT
  type        = string
  default     = "PROVISIONED"

  validation {
    condition     = contains(["PROVISIONED", "PAY_PER_REQUEST"], var.dynamodb_billing_mode)
    error_message = "dynamodb_billing_mode must be PROVISIONED or PAY_PER_REQUEST."
  }
}

variable "provisioned_capacity" {
  description = "Read/write capacity per table when PROVISIONED. Totals must stay <= 25 each to remain free."
  type        = map(object({ read = number, write = number }))
  default = {
    transactions  = { read = 1, write = 10 } # the app never reads it; Lambda reads the stream for free
    account_state = { read = 10, write = 10 }
    alerts        = { read = 1, write = 4 }
  }

  validation {
    condition = (
      sum([for t in values(var.provisioned_capacity) : t.read]) <= 25 &&
      sum([for t in values(var.provisioned_capacity) : t.write]) <= 25
    )
    error_message = "Total provisioned read and write capacity must each be <= 25 to stay in the free tier."
  }
}

variable "rule_overrides" {
  description = "Detection threshold overrides passed to the detector, e.g. { velocity_min_count = \"8\" }."
  type        = map(string)
  default     = {}
}

variable "notify_min_severity" {
  description = "Lowest alert severity that triggers an email."
  type        = string
  default     = "CRITICAL"

  validation {
    condition     = contains(["LOW", "MEDIUM", "HIGH", "CRITICAL"], var.notify_min_severity)
    error_message = "notify_min_severity must be LOW, MEDIUM, HIGH or CRITICAL."
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention. Logs never expire by default, which slowly costs money."
  type        = number
  default     = 14
}

variable "athena_bytes_scanned_cutoff" {
  description = "Athena cancels any query that would scan more than this (bytes). 1 GB costs about $0.005."
  type        = number
  default     = 1073741824
}

variable "lake_start_date" {
  description = "Earliest event_date Athena looks for (partition projection). Simulated data must be after it."
  type        = string
  default     = "2026-01-01"
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget. You are emailed on the first cent of actual spend and on a forecast above this."
  type        = string
  default     = "1"
}

variable "force_destroy_buckets" {
  description = "Let `terraform destroy` delete the S3 buckets even when they contain data."
  type        = bool
  default     = true
}
