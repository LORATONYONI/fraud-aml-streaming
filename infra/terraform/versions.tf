terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # State is kept locally (terraform.tfstate, git-ignored) to stay free and simple.
  # For a team, move it to an S3 backend with `use_lockfile = true`.
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  prefix         = var.project_name
  glue_prefix    = replace(var.project_name, "-", "_")
  account_id     = data.aws_caller_identity.current.account_id
  provisioned    = var.dynamodb_billing_mode == "PROVISIONED"
  email_alerts   = var.alert_email != ""
  repo_root      = "${path.module}/../.."
  lake_bucket    = "${var.project_name}-lake-${local.account_id}-${var.aws_region}"
  results_bucket = "${var.project_name}-athena-${local.account_id}-${var.aws_region}"
}
