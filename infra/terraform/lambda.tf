# One zip for all three functions. The code has no third-party dependencies (boto3 ships with the
# Lambda runtime), so there is no layer or container image to build. Local-only code (the producer
# and the Kafka mode) is left out.
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${local.repo_root}/src"
  output_path = "${path.module}/build/lambda.zip"
  excludes = [
    "fraud_aml/producer", "fraud_aml/producer/**",
    "fraud_aml/kafka", "fraud_aml/kafka/**", "fraud_aml/sqlite_store.py",
    "**/__pycache__/**", "**/*.pyc",
  ]
}

locals {
  functions = {
    detector = {
      description = "Runs fraud/AML rules on new transactions and stores alerts"
      handler     = "fraud_aml.handlers.detector.handler"
      memory      = 256
      timeout     = 60
      env = {
        STATE_TABLE    = aws_dynamodb_table.account_state.name
        ALERTS_TABLE   = aws_dynamodb_table.alerts.name
        RULE_OVERRIDES = jsonencode(var.rule_overrides)
        STATE_TTL_DAYS = "30"
        ALERT_TTL_DAYS = "30"
      }
    }
    archiver = {
      description = "Writes transactions and alerts to the S3 bronze layer"
      handler     = "fraud_aml.handlers.archiver.handler"
      memory      = 256
      timeout     = 120
      env = {
        LAKE_BUCKET = aws_s3_bucket.lake.id
        ENTITY_BY_TABLE = jsonencode({
          (aws_dynamodb_table.transactions.name) = "transactions"
          (aws_dynamodb_table.alerts.name)       = "alerts"
        })
      }
    }
    notifier = {
      description = "Emails high-severity alerts through SNS"
      handler     = "fraud_aml.handlers.notifier.handler"
      memory      = 128
      timeout     = 30
      env = {
        ALERT_TOPIC_ARN = aws_sns_topic.fraud_alerts.arn
        MIN_SEVERITY    = var.notify_min_severity
      }
    }
  }

  # Each stream has at most two consumers, the limit DynamoDB recommends.
  stream_triggers = {
    detector_transactions = { function = "detector", stream = aws_dynamodb_table.transactions.stream_arn, batch_size = 100, window_s = 5 }
    archiver_transactions = { function = "archiver", stream = aws_dynamodb_table.transactions.stream_arn, batch_size = 1000, window_s = 60 }
    archiver_alerts       = { function = "archiver", stream = aws_dynamodb_table.alerts.stream_arn, batch_size = 1000, window_s = 60 }
    notifier_alerts       = { function = "notifier", stream = aws_dynamodb_table.alerts.stream_arn, batch_size = 100, window_s = 10 }
  }
}

# Created explicitly so logs expire; Lambda's auto-created log groups keep logs forever.
resource "aws_cloudwatch_log_group" "fn" {
  for_each          = local.functions
  name              = "/aws/lambda/${local.prefix}-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "fn" {
  for_each         = local.functions
  function_name    = "${local.prefix}-${each.key}"
  description      = each.value.description
  role             = aws_iam_role.fn[each.key].arn
  runtime          = "python3.13"
  architectures    = ["arm64"] # Graviton: ~20% cheaper per GB-second, same free tier
  handler          = each.value.handler
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  memory_size      = each.value.memory
  timeout          = each.value.timeout

  environment {
    variables = each.value.env
  }

  logging_config {
    log_format            = "JSON"
    log_group             = aws_cloudwatch_log_group.fn[each.key].name
    application_log_level = "INFO"
    system_log_level      = "WARN"
  }

  depends_on = [aws_iam_role_policy.fn]
}

resource "aws_lambda_event_source_mapping" "stream" {
  for_each         = local.stream_triggers
  function_name    = aws_lambda_function.fn[each.value.function].arn
  event_source_arn = each.value.stream
  # TRIM_HORIZON, not LATEST: a new trigger takes about a minute to start polling, and with LATEST
  # everything written in that minute is silently skipped (seen on the first deploy: about 300 of
  # 621 transactions). Re-reading the stream's 24h history is safe because processing is idempotent.
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = each.value.batch_size
  maximum_batching_window_in_seconds = each.value.window_s

  # Failure handling: split a failing batch to isolate the bad record, retry a few times,
  # then record the failed batch's location in the dead-letter queue instead of blocking the shard.
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 21600

  # Only new items. TTL deletions (REMOVE events) never invoke the functions.
  filter_criteria {
    filter {
      pattern = jsonencode({ eventName = ["INSERT"] })
    }
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.fn]
}
