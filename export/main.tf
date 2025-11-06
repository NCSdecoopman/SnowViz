terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
    archive = { source = "hashicorp/archive", version = ">= 2.4.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" { default = "eu-west-3" }
variable "table_name" { default = "Observations" }
variable "repo_owner" { default = "NCSdecoopman" }
variable "repo_name"  { default = "SnowViz" }
variable "branch"     { default = "main" }
variable "secret_name" { default = "SnowViz-AutoUpdate-AWS" }

data "aws_caller_identity" "me" {}
data "aws_region" "cur" {}

# Zip du code
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda_function.py"
  output_path = "${path.module}/lambda.zip"
}

# Rôle Lambda
resource "aws_iam_role" "lambda_role" {
  name = "ddb-export-snowviz-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

# Politique IAM minimale
resource "aws_iam_policy" "lambda_policy" {
  name        = "ddb-export-snowviz-policy"
  description = "DynamoDB Scan, Secrets read, Logs"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid: "DdbScan",
        Effect: "Allow",
        Action: ["dynamodb:Scan"],
        Resource: "arn:aws:dynamodb:${data.aws_region.cur.name}:${data.aws_caller_identity.me.account_id}:table/${var.table_name}"
      },
      {
        Sid: "SecretsRead",
        Effect: "Allow",
        Action: ["secretsmanager:GetSecretValue"],
        Resource: "arn:aws:secretsmanager:${data.aws_region.cur.name}:${data.aws_caller_identity.me.account_id}:secret:${var.secret_name}*"
      },
      {
        Sid: "Logs",
        Effect: "Allow",
        Action: ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
        Resource: "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Log group (rétention 7 jours)
resource "aws_cloudwatch_log_group" "lg" {
  name              = "/aws/lambda/ddb-export-observations-to-github"
  retention_in_days = 7
}

# Fonction Lambda
resource "aws_lambda_function" "exporter" {
  function_name = "ddb-export-observations-to-github"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  filename      = data.archive_file.lambda_zip.output_path
  timeout       = 900
  memory_size   = 512

  environment {
    variables = {
      TABLE_NAME             = var.table_name
      GH_OWNER               = var.repo_owner
      GH_REPO                = var.repo_name
      GH_BRANCH              = var.branch
      GH_PATH                = "data/observations.json"
      GH_TOKEN_SECRET_ARN    = "arn:aws:secretsmanager:${data.aws_region.cur.name}:${data.aws_caller_identity.me.account_id}:secret:${var.secret_name}"
      DDB_PROJECTION         = "id,#d,HNEIGEF,NEIGETOT,NEIGETOT06"
      MAX_JSON_MB            = "100"
      FALLBACK_GZ_PATH       = "data/observations.json.gz"
    }
  }

  depends_on = [aws_iam_role_policy_attachment.attach, aws_cloudwatch_log_group.lg]
}

# EventBridge: tous les jours à 07:00 UTC
# => 08:00 à Paris en hiver (UTC+1), 09:00 en été (UTC+2)
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "ddb-export-observations-daily"
  schedule_expression = "cron(00 7 * * ? *)"
}

resource "aws_cloudwatch_event_target" "tgt" {
  rule      = aws_cloudwatch_event_rule.daily.name
  target_id = "lambda"
  arn       = aws_lambda_function.exporter.arn
}

resource "aws_lambda_permission" "allow_events" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.exporter.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}
