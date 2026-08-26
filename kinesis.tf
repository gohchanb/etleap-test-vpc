# Kinesis streams + the IAM role Etleap assumes to read from them.
# Setup follows https://docs.etleap.com/documentation/sources/events/kinesis/#kinesis

locals {
  kinesis_stream_count = 0
}

# Kinesis data streams, provisioned capacity (1 shard each).
# Provisioned avoids the default 50 on-demand-stream-per-account limit.
resource "aws_kinesis_stream" "etleap_test" {
  count = local.kinesis_stream_count

  name = format("test2-stream-%02d", count.index + 1)

  retention_period = 2160 # 90 days, in hours

  shard_count = 1

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = "sandbox"
    Operator    = "Benedict.Chan"
  }
}

# Trust policy: allow Etleap's AWS accounts to assume this role, each gated by its own External ID.
# Account IDs and External IDs come from the Etleap Kinesis connection setup page.
# Separate statements are required because each account pairs with a different External ID.
data "aws_iam_policy_document" "etleap_kinesis_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::209479308486:root"] # Etleap's Account ID
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["test2"] # External ID
    }
  }

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::223848809711:root"] # Etleap's Account ID
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["etleap"] # External ID
    }
  }
}

resource "aws_iam_role" "etleap_kinesis" {
  name               = "etleap_kinesis_access"
  assume_role_policy = data.aws_iam_policy_document.etleap_kinesis_assume_role.json

  tags = {
    Environment = "sandbox"
    Operator    = "Benedict.Chan"
  }
}

# Permissions Etleap needs to list and read the streams.
data "aws_iam_policy_document" "etleap_kinesis_access" {
  statement {
    effect = "Allow"
    actions = [
      "kinesis:ListStreams",
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "etleap_kinesis_access" {
  name   = "etleap_kinesis_access_policy"
  role   = aws_iam_role.etleap_kinesis.id
  policy = data.aws_iam_policy_document.etleap_kinesis_access.json
}

# NOTE: If you restrict assumable roles via the VPC module's `roles_allowed_to_be_assumed`
# input variable, add aws_iam_role.etleap_kinesis.arn to that list (see the doc warning).

output "etleap_kinesis_role_arn" {
  value = aws_iam_role.etleap_kinesis.arn
}

output "etleap_kinesis_stream_names" {
  value = aws_kinesis_stream.etleap_test[*].name
}
