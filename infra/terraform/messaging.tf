# Where failed stream batches are recorded (location only, not the records) for later replay.
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-stream-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true
}

# Fraud alerts for investigators.
resource "aws_sns_topic" "fraud_alerts" {
  name = "${local.prefix}-fraud-alerts"
}

# Pipeline health (Lambda errors, stream lag, dead letters) for whoever runs the platform.
resource "aws_sns_topic" "ops" {
  name = "${local.prefix}-ops"
}

# Email subscriptions stay "pending" until you click the confirmation link AWS sends.
resource "aws_sns_topic_subscription" "fraud_alerts_email" {
  count     = local.email_alerts ? 1 : 0
  topic_arn = aws_sns_topic.fraud_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sns_topic_subscription" "ops_email" {
  count     = local.email_alerts ? 1 : 0
  topic_arn = aws_sns_topic.ops.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
