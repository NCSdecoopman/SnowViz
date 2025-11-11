terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws     = { source = "hashicorp/aws", version = ">= 5.0" }
    archive = { source = "hashicorp/archive", version = ">= 2.4.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

########################
# Variables
########################
variable "aws_region" {
  type    = string
  default = "eu-west-3"
}
variable "table_name" {
  type    = string
  default = "Observations"
}
variable "repo_owner" {
  type    = string
  default = "NCSdecoopman"
}
variable "repo_name" {
  type    = string
  default = "niveo"
}
variable "branch" {
  type    = string
  default = "main"
}
variable "param_name" {
  type    = string
  default = "/snowviz/github/pat"  # ex. SSM path
}
# Active le déclencheur EventBridge quotidien à 07:00 UTC si true -var="enable_schedule=true"
variable "enable_schedule" {
  type    = bool
  default = false
}

data "aws_caller_identity" "me" {}
data "aws_region" "cur" {}

########################
# Packaging Lambda
########################
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda_function.py"
  output_path = "${path.module}/lambda.zip"
}

########################
# LOGS
########################
resource "aws_cloudwatch_log_group" "lg" {
  name              = "/aws/lambda/ddb-export-observations-to-github"
  retention_in_days = 7
  skip_destroy      = true
}

########################
# IAM
########################
resource "aws_iam_role" "lambda_role" {
  name = "ddb-export-snowviz-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

data "aws_kms_alias" "aws_ssm" { name = "alias/aws/ssm" }

resource "aws_iam_policy" "lambda_policy" {
  name        = "ddb-export-snowviz-policy"
  description = "DynamoDB Scan, SSM read, Logs"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid   = "DdbScan",
        Effect= "Allow",
        Action= ["dynamodb:Scan"],
        Resource = "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.me.account_id}:table/${var.table_name}"
      },
      {
        Sid   = "SsmRead",
        Effect= "Allow",
        Action= ["ssm:GetParameter","ssm:GetParameters"],
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.me.account_id}:parameter${var.param_name}"
      },
      {
        Sid   = "KmsDecryptForSSM",
        Effect= "Allow",
        Action= ["kms:Decrypt"],
        Resource = data.aws_kms_alias.aws_ssm.target_key_arn
      },
      {
        Sid   = "Logs",
        Effect= "Allow",
        Action= ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

########################
# Lambda
########################
data "aws_kms_alias" "aws_lambda" { name = "alias/aws/lambda" }

resource "aws_lambda_function" "exporter" {
  function_name = "ddb-export-observations-to-github"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  filename      = data.archive_file.lambda_zip.output_path
  timeout       = 900
  memory_size   = 512
  kms_key_arn   = data.aws_kms_alias.aws_lambda.target_key_arn

  environment {
    variables = {
      TABLE_NAME           = var.table_name
      GH_OWNER             = var.repo_owner
      GH_REPO              = var.repo_name
      GH_BRANCH            = var.branch
      GH_PATH              = "data/observations.json"
      GH_TOKEN_PARAM_NAME  = var.param_name
      DDB_PROJECTION       = "id,#d,HNEIGEF,NEIGETOT,NEIGETOT06"
      MAX_JSON_MB          = "100"
      FALLBACK_GZ_PATH     = "data/observations.json.gz"
    }
  }

  depends_on = [aws_iam_role_policy_attachment.attach]
}

########################
# EventBridge (optionnel)
########################
# cron(Minutes Heures Jour-du-mois Mois Jour-de-semaine Année)
# 07:00 UTC tous les jours = cron(0 7 * * ? *) = 08:00 heure FR en hiver (09:00 en été)
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "ddb-export-observations-daily"
  schedule_expression = "cron(0 7 * * ? *)"
}

resource "aws_cloudwatch_event_target" "tgt" {
  rule      = aws_cloudwatch_event_rule.daily.name
  target_id = "lambda"
  arn       = aws_lambda_function.exporter.arn
}

resource "random_id" "sid" { byte_length = 4 }

resource "aws_lambda_permission" "allow_events_daily" {
  statement_id  = "AllowFromEvents-ddb-export-observations-daily-${md5(timestamp())}" # Génère un ID unique
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.exporter.arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
  depends_on    = [aws_lambda_function.exporter, aws_cloudwatch_event_rule.daily]
}
