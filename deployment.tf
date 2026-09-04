provider "aws" {
  allowed_account_ids = ["209479308486"] # Require that this is run only in the sandbox account
}

resource "aws_key_pair" "ssh" {
  key_name   = "Etleap-Benedict" # !! Replace the name with your name to avoid conflicts.
  public_key = file("test.pub") # !! Replace this with a key where you have access to the coresponding private key. You can also generate a new one with `ssh-keygen -f "file-name"` and following the prompts. 
}

# resource "aws_secretsmanager_secret" "github_token" {
#   name = "EtleapGithubToken-SandboxTest2"
# }

# resource "aws_s3_bucket" "emr-scripts" {
#   bucket        = "etleap-test2-emr-scripts"
#   force_destroy = true

#   lifecycle {
#     ignore_changes = [
#       bucket,
#       logging
#     ]
#   }
# }

# resource "aws_kms_key" "s3_encryption" {
#   description = "Test 2 KMS key for S3 bucket encryption"
# }

module "vpc" {
  # source = "git@github.com:etleap/terraform-aws-etleap-vpc-private.git?ref=vpc-nat-saturation-configurable-value-vik-8075" # This can also be changed to use private repo or even branch of it e.g. "git@github.com:etleap/terraform-aws-etleap-vpc-private.git?ref=vikings/job-monitoring-vpc#178788287" or it can also be just your local directory containing vpc module, e.g. `../terraform-aws-etleap-vpc-private`
  source = "etleap/etleap-vpc/aws"
  version = "1.19.0" # make sure to update to the latest one (or remove if using branch as a source)

  deployment_id = "test2" # !! Replace with the correct one

  # github_username = "gohchanb"
  # github_access_token_arn = aws_secretsmanager_secret.github_token.arn

  # to create a new VPC, keep this as is
  vpc_cidr_block_1 = 172
  vpc_cidr_block_2 = 22
  vpc_cidr_block_3 = 4

  # to use the existing Sandbox VPC, comment the var_cird_block_* variables above and uncomment below
  # TODO: these were working for test account - but we do not have new ones yet - if/when re-testing this method please populate with new VPCs/subnets.
  # vpc_id                  = "vpc-0820c79baa3720297"
  # public_subnets          = ["subnet-0da4febe81aaf66aa", "subnet-051f89cb895df1a57", "subnet-099e7c385b534967e"]
  # private_subnets         = ["subnet-067f0791d940b3385", "subnet-0868b90b289777752", "subnet-037f37825d88724c6"]

  # key_name   = aws_key_pair.ssh.key_name
  # !! Set your name and email here. The domain is not "etleap.com", as we don't allow that
  first_name = "Benedict"
  last_name  = "Chan"
  email      = "benedict@etleap-dev.com"

  resource_tags = {
    Environment = "sandbox",
    Operator   = "Benedict.Chan", # !! Put you name here, so we know who owns the resources
  }

  # s3_kms_encryption_key = aws_kms_key.s3_encryption.arn

  dms_roles_to_be_created = false

  s3_input_buckets = ["benedict-dev-test"]

  app_instance_type = "c7a.4xlarge"

  # connection_secrets = {
  #   ETLEAP_SECRET_BING_ADS_CLIENT_ID                  = aws_secretsmanager_secret.bing_ads_client_id.arn,
  #   ETLEAP_SECRET_BING_ADS_CLIENT_SECRET              = aws_secretsmanager_secret.bing_ads_client_secret.arn,
  #   ETLEAP_SECRET_HUBSPOT_CLIENT_ID                   = aws_secretsmanager_secret.hubspot_client_id.arn,
  #   ETLEAP_SECRET_HUBSPOT_CLIENT_SECRET               = aws_secretsmanager_secret.hubspot_client_secret.arn,
  #   ETLEAP_SECRET_GOOGLE_ANALYTICS_GA4_CLIENT_ID      = aws_secretsmanager_secret.google_analytics_client_id.arn,
  #   ETLEAP_SECRET_GOOGLE_ANALYTICS_GA4_CLIENT_SECRET  = aws_secretsmanager_secret.google_analytics_client_secret.arn,
  #   ETLEAP_SECRET_JIRA_PRIVATE                        = aws_secretsmanager_secret.hubspot_jira_private_key.arn,
  #   ETLEAP_SECRET_SALESFORCE_V2_CLIENT_SECRET         = aws_secretsmanager_secret.salesforce_v2_client_secret.arn
  # }
}

# The DNS to access the app at.
output "app-hostname" {
  value = module.vpc.app_public_address
}

# Randomly generated setup password; see below on how to read it
output "setup-password" {
  sensitive = true
  value     = module.vpc.setup_password
}

output "kms-policy" {
  value     = module.vpc.kms_policy
}

# resource "aws_secretsmanager_secret" "bing_ads_client_id" {
#     name = "EtleapBingAdsClientId"
# }

# resource "aws_secretsmanager_secret" "bing_ads_client_secret" {
#     name = "EtleapBingAdsClientSecret"
# }

# resource "aws_secretsmanager_secret" "hubspot_client_id" {
#     name = "EtleapHubspotClientId"
# }

# resource "aws_secretsmanager_secret" "hubspot_client_secret" {
#     name = "EtleapHubspotClientSecret"
# }

# resource "aws_secretsmanager_secret" "google_analytics_client_id" {
#   name = "EtleapGoogleAnalyticsClientId"
# }

# resource "aws_secretsmanager_secret" "google_analytics_client_secret" {
#   name = "EtleapGoogleAnalyticsClientSecret"
# }

# resource "aws_secretsmanager_secret" "hubspot_jira_private_key" {
#   name = "EtleapJiraPrivateKey"
# }

# resource "aws_secretsmanager_secret" "salesforce_v2_client_secret" {
#     name = "EtleapSalesforceV2ClientSecret"
# }

# resource "aws_secretsmanager_secret" "google_sheets_client_secret" {
#     name = "EtleapGoogleSheetsClientSecret"
# }

output "s3_input_role_arn" {
  value       = module.vpc.s3_input_role_arn
}

output "s3_input_bucket_policy" {
  value       = module.vpc.s3_input_bucket_policy
}