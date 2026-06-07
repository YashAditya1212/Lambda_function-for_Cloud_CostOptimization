# EBS Snapshot Cleanup — Lambda Project

Automatically deletes EBS snapshots older than 30 days  
whose source volume is **not attached to any EC2 instance**.

---

## What It Does

```
EventBridge (every Sunday midnight UTC)
          ↓
    Lambda Function
          ↓
1. Collect all volume IDs attached to EC2 instances (running + stopped)
2. Find all snapshots older than RETENTION_DAYS
3. Delete snapshots whose volume is NOT in the attached list
4. Publish summary report to SNS (optional)
```

---

## Files

| File | Purpose |
|---|---|
| `snapshot_cleanup.py` | Lambda function code |
| `deploy_lambda.py` | Deploys everything to AWS from scratch |
| `requirements.txt` | Python dependencies |

---

## Quick Start

```bash
# 1 — Install dependencies
pip install -r requirements.txt

# 2 — Configure AWS credentials
aws configure

# 3 — Deploy (starts in dry-run mode — safe)
python deploy_lambda.py

# 4 — Review dry-run output in CloudWatch Logs
aws logs tail /aws/lambda/medisync-snapshot-cleanup --follow

# 5 — Enable live mode once satisfied
aws lambda update-function-configuration \
  --function-name medisync-snapshot-cleanup \
  --environment "Variables={RETENTION_DAYS=30,DRY_RUN=false,SNS_TOPIC_ARN=}"
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `RETENTION_DAYS` | `30` | Delete snapshots older than this many days |
| `DRY_RUN` | `true` | `true` = simulate only, `false` = live deletes |
| `SNS_TOPIC_ARN` | `""` | SNS topic ARN for reports (leave empty to skip) |
| `AWS_REGION` | `ap-south-1` | AWS region (set automatically by Lambda) |

---

## IAM Permissions Required

The deploy script creates this role automatically.

```
ec2:DescribeInstances
ec2:DescribeVolumes
ec2:DescribeSnapshots
ec2:DeleteSnapshot
sns:Publish
logs:CreateLogGroup
logs:CreateLogStream
logs:PutLogEvents
```

---

## Safety Design

- **Dry-run by default** — will never delete anything until you explicitly set `DRY_RUN=false`
- **Skips attached volumes** — snapshots for volumes attached to any instance (running OR stopped) are never touched
- **Skips in-use snapshots** — if a snapshot is being used to create an AMI or volume, it is skipped with a warning
- **Continues on error** — a failure on one snapshot does not abort the rest
- **Full report** — every run logs what was deleted (or would be deleted in dry-run)

---

## Schedule

Runs every **Sunday at midnight UTC** via EventBridge.

To change the schedule after deployment:

```bash
aws events put-rule \
  --name medisync-snapshot-cleanup-weekly \
  --schedule-expression "cron(0 0 ? * MON *)"
```

---

## Manual Trigger

```bash
aws lambda invoke \
  --function-name medisync-snapshot-cleanup \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  response.json && cat response.json
```
