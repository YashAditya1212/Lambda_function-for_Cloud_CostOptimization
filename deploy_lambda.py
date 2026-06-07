"""
deploy_lambda.py

Deploys the snapshot_cleanup Lambda function to AWS from scratch.

What this script does:
    1. Creates an IAM execution role with least-privilege permissions
    2. Packages snapshot_cleanup.py into a deployment zip
    3. Creates (or updates) the Lambda function
    4. Creates an EventBridge rule to trigger it every Sunday at midnight UTC
    5. Runs a test invocation in dry-run mode

Usage:
    python deploy_lambda.py

Requirements:
    pip install boto3

AWS credentials must be configured:
    Option A — run 'aws configure'
    Option B — export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    Option C — run this script from an EC2 instance with an IAM role attached
"""

import boto3
import json
import zipfile
import io
import os
import sys
import time
import logging
from botocore.exceptions import ClientError

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%H:%M:%S",
    level   = logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
FUNCTION_NAME   = "medisync-snapshot-cleanup"
HANDLER         = "snapshot_cleanup.lambda_handler"
RUNTIME         = "python3.12"
TIMEOUT_SECONDS = 300           # 5 minutes — accounts with many snapshots need time
MEMORY_MB       = 256
REGION          = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
ROLE_NAME       = "medisync-lambda-snapshot-cleanup-role"
SOURCE_FILE     = "snapshot_cleanup.py"

# EventBridge schedule — every Sunday at midnight UTC
# Cron format: cron(minute hour day-of-month month day-of-week year)
SCHEDULE_EXPRESSION = "cron(0 0 ? * SUN *)"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — IAM Role
# ─────────────────────────────────────────────────────────────────────────────

def ensure_iam_role(iam_client):
    """
    Create the Lambda execution role if it doesn't exist.
    Returns the role ARN.
    """

    # Trust policy — allows Lambda service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":    "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action":    "sts:AssumeRole",
        }]
    }

    # Inline policy — least privilege for this specific function
    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid":    "EC2ReadDescribe",
                "Effect": "Allow",
                "Action": [
                    "ec2:DescribeInstances",
                    "ec2:DescribeVolumes",
                    "ec2:DescribeSnapshots",
                ],
                "Resource": "*",   # describe calls don't support resource-level permissions
            },
            {
                "Sid":    "SnapshotDelete",
                "Effect": "Allow",
                "Action": [
                    "ec2:DeleteSnapshot",
                ],
                "Resource": "*",
            },
            {
                "Sid":    "SNSPublish",
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": "*",   # restrict to your specific topic ARN in production
            },
            {
                "Sid":    "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ]
    }

    # Check if role already exists
    try:
        role     = iam_client.get_role(RoleName=ROLE_NAME)
        role_arn = role["Role"]["Arn"]
        logger.info("IAM role already exists: %s", role_arn)
        return role_arn

    except iam_client.exceptions.NoSuchEntityException:
        pass   # role doesn't exist — create it below

    # Create the role
    logger.info("Creating IAM role: %s", ROLE_NAME)

    role = iam_client.create_role(
        RoleName                 = ROLE_NAME,
        AssumeRolePolicyDocument = json.dumps(trust_policy),
        Description              = "Execution role for EBS snapshot cleanup Lambda",
        Tags                     = [
            {"Key": "Project",   "Value": "medisync"},
            {"Key": "ManagedBy", "Value": "deploy_lambda.py"},
        ],
    )

    role_arn = role["Role"]["Arn"]

    # Attach the inline policy
    iam_client.put_role_policy(
        RoleName       = ROLE_NAME,
        PolicyName     = "SnapshotCleanupPermissions",
        PolicyDocument = json.dumps(inline_policy),
    )

    logger.info("IAM role created: %s", role_arn)
    logger.info("Waiting 15s for IAM role to propagate across AWS...")
    time.sleep(15)   # IAM changes take a moment to be globally consistent

    return role_arn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Package code into zip
# ─────────────────────────────────────────────────────────────────────────────

def create_deployment_zip(source_file=SOURCE_FILE):
    """
    Zip the Lambda source file in memory and return raw bytes.
    No temp files needed — we upload directly from memory.
    """
    if not os.path.exists(source_file):
        logger.error("Source file not found: %s", source_file)
        logger.error("Make sure %s is in the same directory as this script", source_file)
        sys.exit(1)

    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(source_file, arcname=source_file)

    buffer.seek(0)
    zip_bytes = buffer.read()

    logger.info(
        "Packaged %s → zip (%d KB)",
        source_file, len(zip_bytes) // 1024
    )
    return zip_bytes


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Deploy Lambda (create or update)
# ─────────────────────────────────────────────────────────────────────────────

def deploy_lambda(lam_client, role_arn, zip_bytes):
    """
    Create the Lambda function if it doesn't exist.
    Update code and configuration if it does.
    Returns the function ARN.
    """

    env_vars = {
        "RETENTION_DAYS": "30",
        "DRY_RUN":        "true",    # ALWAYS start with dry run — verify first!
        "SNS_TOPIC_ARN":  "",        # fill in your SNS topic ARN here
    }

    tags = {
        "Project":     "medisync",
        "ManagedBy":   "deploy_lambda.py",
        "Environment": "production",
    }

    try:
        # Check if function already exists
        existing = lam_client.get_function(FunctionName=FUNCTION_NAME)
        logger.info("Function exists — updating: %s", FUNCTION_NAME)

        # Update code
        lam_client.update_function_code(
            FunctionName = FUNCTION_NAME,
            ZipFile      = zip_bytes,
            Publish      = True,
        )

        # Wait for the update to finish before changing config
        logger.info("Waiting for code update to complete...")
        waiter = lam_client.get_waiter("function_updated")
        waiter.wait(
            FunctionName = FUNCTION_NAME,
            WaiterConfig = {"Delay": 5, "MaxAttempts": 30},
        )

        # Update configuration
        lam_client.update_function_configuration(
            FunctionName = FUNCTION_NAME,
            Timeout      = TIMEOUT_SECONDS,
            MemorySize   = MEMORY_MB,
            Environment  = {"Variables": env_vars},
        )

        func_arn = existing["Configuration"]["FunctionArn"]
        logger.info("Lambda updated ✓  ARN: %s", func_arn)
        return func_arn

    except lam_client.exceptions.ResourceNotFoundException:
        # Function doesn't exist — create it fresh
        logger.info("Creating new Lambda function: %s", FUNCTION_NAME)

        response = lam_client.create_function(
            FunctionName  = FUNCTION_NAME,
            Runtime       = RUNTIME,
            Role          = role_arn,
            Handler       = HANDLER,
            Code          = {"ZipFile": zip_bytes},
            Timeout       = TIMEOUT_SECONDS,
            MemorySize    = MEMORY_MB,
            Description   = (
                "Deletes EBS snapshots older than RETENTION_DAYS days "
                "for volumes not attached to any EC2 instance."
            ),
            Environment   = {"Variables": env_vars},
            Tags          = tags,
        )

        func_arn = response["FunctionArn"]

        # Wait for function to become active before adding triggers
        logger.info("Waiting for Lambda to become active...")
        waiter = lam_client.get_waiter("function_active")
        waiter.wait(
            FunctionName = FUNCTION_NAME,
            WaiterConfig = {"Delay": 5, "MaxAttempts": 30},
        )

        logger.info("Lambda created ✓  ARN: %s", func_arn)
        return func_arn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — EventBridge Schedule
# ─────────────────────────────────────────────────────────────────────────────

def create_eventbridge_schedule(events_client, lam_client, function_arn):
    """
    Create an EventBridge rule that triggers the Lambda
    every Sunday at midnight UTC.
    """
    rule_name = f"{FUNCTION_NAME}-weekly"

    # Create the schedule rule
    try:
        rule = events_client.put_rule(
            Name               = rule_name,
            ScheduleExpression = SCHEDULE_EXPRESSION,
            State              = "ENABLED",
            Description        = (
                f"Trigger {FUNCTION_NAME} every Sunday at midnight UTC. "
                f"Deletes stale EBS snapshots."
            ),
        )
        rule_arn = rule["RuleArn"]
        logger.info("EventBridge rule created/updated: %s", rule_arn)

    except ClientError as e:
        logger.error("Failed to create EventBridge rule: %s", e)
        raise

    # Set Lambda as the target
    events_client.put_targets(
        Rule    = rule_name,
        Targets = [{
            "Id":  "SnapshotCleanupLambda",
            "Arn": function_arn,
        }]
    )

    # Grant EventBridge permission to invoke the Lambda
    try:
        lam_client.add_permission(
            FunctionName = FUNCTION_NAME,
            StatementId  = f"allow-eventbridge-{rule_name}",
            Action       = "lambda:InvokeFunction",
            Principal    = "events.amazonaws.com",
            SourceArn    = rule_arn,
        )
        logger.info("EventBridge invoke permission added to Lambda")

    except lam_client.exceptions.ResourceConflictException:
        logger.info("EventBridge permission already exists — skipping")

    logger.info(
        "Schedule configured: %s → triggers every Sunday at midnight UTC",
        rule_name
    )
    return rule_arn


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Test invocation
# ─────────────────────────────────────────────────────────────────────────────

def test_invocation(lam_client):
    """
    Invoke the Lambda synchronously in dry-run mode.
    Validates the function runs without errors.
    """
    logger.info("Running test invocation (dry run mode)...")

    response = lam_client.invoke(
        FunctionName   = FUNCTION_NAME,
        InvocationType = "RequestResponse",   # synchronous
        Payload        = json.dumps({}),
    )

    payload     = json.loads(response["Payload"].read())
    status_code = response["StatusCode"]
    func_error  = response.get("FunctionError")

    if func_error:
        logger.error("Lambda returned a function error: %s", func_error)
        logger.error("Payload: %s", json.dumps(payload, indent=2))
        return False

    if status_code == 200:
        logger.info("Test invocation successful ✓")
        logger.info("Result: %s", json.dumps(payload, indent=2))
        return True

    logger.warning("Unexpected status code: %d", status_code)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 55)
    print("  EBS Snapshot Cleanup — Lambda Deployment")
    print("=" * 55)
    print(f"  Function : {FUNCTION_NAME}")
    print(f"  Region   : {REGION}")
    print(f"  Runtime  : {RUNTIME}")
    print(f"  Schedule : Every Sunday at midnight UTC")
    print("=" * 55)
    print()

    # Initialise clients
    iam    = boto3.client("iam",    region_name=REGION)
    lam    = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)

    # ── Step 1 — IAM Role ─────────────────────────────────────────────────────
    logger.info("Step 1/5 — Ensuring IAM execution role exists...")
    role_arn = ensure_iam_role(iam)

    # ── Step 2 — Package code ─────────────────────────────────────────────────
    logger.info("Step 2/5 — Packaging Lambda code...")
    zip_bytes = create_deployment_zip(SOURCE_FILE)

    # ── Step 3 — Deploy Lambda ────────────────────────────────────────────────
    logger.info("Step 3/5 — Deploying Lambda function...")
    function_arn = deploy_lambda(lam, role_arn, zip_bytes)

    # ── Step 4 — EventBridge Schedule ─────────────────────────────────────────
    logger.info("Step 4/5 — Setting up EventBridge schedule...")
    create_eventbridge_schedule(events, lam, function_arn)

    # ── Step 5 — Test run ─────────────────────────────────────────────────────
    logger.info("Step 5/5 — Running test invocation...")
    success = test_invocation(lam)

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 55)
    if success:
        print("  ✓ Deployment complete!")
    else:
        print("  ✗ Deployment complete but test invocation failed")
        print("    Check CloudWatch Logs for details:")
        print(f"    /aws/lambda/{FUNCTION_NAME}")
    print()
    print(f"  Function ARN : {function_arn}")
    print(f"  IAM Role     : {ROLE_NAME}")
    print(f"  Schedule     : Every Sunday, 00:00 UTC")
    print()
    print("  IMPORTANT — Next steps:")
    print("  1. Check CloudWatch Logs to review the dry-run output:")
    print(f"     aws logs tail /aws/lambda/{FUNCTION_NAME} --follow")
    print()
    print("  2. Once satisfied, disable dry-run mode:")
    print(f"     aws lambda update-function-configuration \\")
    print(f"       --function-name {FUNCTION_NAME} \\")
    print(f'       --environment "Variables={{RETENTION_DAYS=30,DRY_RUN=false,SNS_TOPIC_ARN=}}"')
    print()
    print("  3. Trigger manually to verify live run:")
    print(f"     aws lambda invoke \\")
    print(f"       --function-name {FUNCTION_NAME} \\")
    print(f"       --payload '{{}}' \\")
    print(f"       --cli-binary-format raw-in-base64-out \\")
    print(f"       response.json && cat response.json")
    print("=" * 55)
    print()


if __name__ == "__main__":
    main()
