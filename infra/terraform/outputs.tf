output "aws_region" {
  value = var.aws_region
}

output "transactions_table_name" {
  description = "Where the producer writes (make produce)."
  value       = aws_dynamodb_table.transactions.name
}

output "alerts_table_name" {
  value = aws_dynamodb_table.alerts.name
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "athena_workgroup" {
  description = "Select this workgroup in the Athena console; the saved queries are under 'Saved queries'."
  value       = aws_athena_workgroup.main.name
}

output "glue_database" {
  value = aws_glue_catalog_database.bronze.name
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dlq.url
}

output "next_steps" {
  value = local.email_alerts ? "Check ${var.alert_email} and confirm the two SNS subscription emails." : "Set alert_email in terraform.tfvars to get alerts, alarms and the cost budget."
}
