# One role per function, each with only the permissions that function needs.

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "fn" {
  for_each           = local.functions
  name               = "${local.prefix}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

locals {
  stream_read_actions = ["dynamodb:DescribeStream", "dynamodb:GetRecords", "dynamodb:GetShardIterator"]
}

data "aws_iam_policy_document" "detector" {
  statement {
    sid       = "ReadTransactionsStream"
    actions   = local.stream_read_actions
    resources = [aws_dynamodb_table.transactions.stream_arn]
  }

  statement {
    sid       = "ListStreams"
    actions   = ["dynamodb:ListStreams"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadWriteAccountState"
    actions   = ["dynamodb:BatchGetItem", "dynamodb:PutItem"]
    resources = [aws_dynamodb_table.account_state.arn]
  }

  statement {
    sid       = "WriteAlerts"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.alerts.arn]
  }

  statement {
    sid       = "FailedBatchesToDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.fn["detector"].arn}:*"]
  }
}

data "aws_iam_policy_document" "archiver" {
  statement {
    sid       = "ReadStreams"
    actions   = local.stream_read_actions
    resources = [aws_dynamodb_table.transactions.stream_arn, aws_dynamodb_table.alerts.stream_arn]
  }

  statement {
    sid       = "ListStreams"
    actions   = ["dynamodb:ListStreams"]
    resources = ["*"]
  }

  statement {
    sid     = "WriteBronze"
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/transactions/*",
      "${aws_s3_bucket.lake.arn}/bronze/alerts/*",
    ]
  }

  statement {
    sid       = "FailedBatchesToDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.fn["archiver"].arn}:*"]
  }
}

data "aws_iam_policy_document" "notifier" {
  statement {
    sid       = "ReadAlertsStream"
    actions   = local.stream_read_actions
    resources = [aws_dynamodb_table.alerts.stream_arn]
  }

  statement {
    sid       = "ListStreams"
    actions   = ["dynamodb:ListStreams"]
    resources = ["*"]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.fraud_alerts.arn]
  }

  statement {
    sid       = "FailedBatchesToDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.fn["notifier"].arn}:*"]
  }
}

locals {
  function_policies = {
    detector = data.aws_iam_policy_document.detector.json
    archiver = data.aws_iam_policy_document.archiver.json
    notifier = data.aws_iam_policy_document.notifier.json
  }
}

resource "aws_iam_role_policy" "fn" {
  for_each = local.functions
  name     = "least-privilege"
  role     = aws_iam_role.fn[each.key].id
  policy   = local.function_policies[each.key]
}
