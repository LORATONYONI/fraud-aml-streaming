# Glue Data Catalog (free under 1M objects) + Athena over the S3 bronze layer.
# Partition projection computes partitions from the path template, so no Glue crawler (billed per
# run) and no MSCK REPAIR TABLE are ever needed.

resource "aws_glue_catalog_database" "bronze" {
  name        = "${local.glue_prefix}_bronze"
  description = "Raw, append-only copies of the DynamoDB streams (at-least-once; de-duplicate on keys)."
}

locals {
  lineage_columns = [
    ["ingest_stream_sequence", "string"],
    ["ingest_archived_at", "string"],
    ["ingest_source_table", "string"],
  ]

  # Money columns are strings on purpose: CAST(amount AS DECIMAL(18,2)) in queries keeps them exact.
  bronze_tables = {
    transactions = {
      description = "Every transaction written to the transactions table, with simulation labels."
      columns = concat([
        ["txn_id", "string"],
        ["event_ts", "string"],
        ["txn_type", "string"],
        ["amount", "string"],
        ["orig_account", "string"],
        ["dest_account", "string"],
        ["orig_balance_before", "string"],
        ["orig_balance_after", "string"],
        ["dest_balance_before", "string"],
        ["dest_balance_after", "string"],
        ["currency", "string"],
        ["source", "string"],
        ["step", "int"],
        ["label_suspicious", "boolean"],
        ["label_scenario", "string"],
        ["label_scenario_id", "string"],
        ["expires_at", "bigint"],
      ], local.lineage_columns)
    }
    alerts = {
      description = "Every alert raised by the detector."
      columns = concat([
        ["account_id", "string"],
        ["alert_key", "string"],
        ["txn_id", "string"],
        ["rule_id", "string"],
        ["severity", "string"],
        ["event_ts", "string"],
        ["detected_at", "string"],
        ["txn_type", "string"],
        ["amount", "string"],
        ["reason", "string"],
        ["details", "string"],
        ["expires_at", "bigint"],
      ], local.lineage_columns)
    }
  }
}

resource "aws_glue_catalog_table" "bronze" {
  for_each      = local.bronze_tables
  name          = each.key
  database_name = aws_glue_catalog_database.bronze.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                              = "TRUE"
    classification                        = "json"
    "projection.enabled"                  = "true"
    "projection.event_date.type"          = "date"
    "projection.event_date.format"        = "yyyy-MM-dd"
    "projection.event_date.range"         = "${var.lake_start_date},NOW+30DAYS"
    "projection.event_date.interval"      = "1"
    "projection.event_date.interval.unit" = "DAYS"
    "storage.location.template"           = "s3://${aws_s3_bucket.lake.id}/bronze/${each.key}/event_date=$${event_date}/"
  }

  partition_keys {
    name = "event_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/bronze/${each.key}/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

resource "aws_athena_workgroup" "main" {
  name          = local.prefix
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Every file in sql/ becomes a saved query in the Athena console.
resource "aws_athena_named_query" "saved" {
  for_each  = fileset("${local.repo_root}/sql", "*.sql")
  name      = trimsuffix(each.value, ".sql")
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.bronze.name
  query     = templatefile("${local.repo_root}/sql/${each.value}", { database = aws_glue_catalog_database.bronze.name })
}
