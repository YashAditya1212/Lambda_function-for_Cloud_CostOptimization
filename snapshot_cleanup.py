"""
snapshot_cleanup.py

Lambda function that:
1. Finds all EBS volumes currently attached to EC2 instances (running + stopped)
2. Finds all snapshots older than RETENTION_DAYS (default: 30)
3. Deletes snapshots whose source volume is NOT attached to any instance
4. Publishes a summary report to SNS

Environment Variables:
    RETENTION_DAYS  : Days to keep snapshots (default: 30)
    DRY_RUN         : "true" to simulate without deleting (default: "true")
    SNS_TOPIC_ARN   : SNS topic ARN for reports (optional — leave empty to skip)
    AWS_REGION      : AWS region (set automatically by Lambda runtime)

Architecture:
    EventBridge (cron: every Sunday midnight UTC)
         ↓  triggers
    This Lambda Function
         ↓  calls
    1. ec2.describe_instances()   → collect all attached volume IDs
    2. ec2.describe_snapshots()   → all snapshots older than RETENTION_DAYS
    3. For each old snapshot:
         if source volume NOT in attached_volumes → delete it
    4. sns.publish()              → summary report
"""

import boto3
import os
import logging
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config from environment variables ────────────────────────────────────────
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))
DRY_RUN        = os.environ.get("DRY_RUN", "true").lower() == "true"
SNS_TOPIC_ARN  = os.environ.get("SNS_TOPIC_ARN", "")
REGION         = os.environ.get("AWS_REGION", "ap-south-1")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Collect all volume IDs attached to instances
# ─────────────────────────────────────────────────────────────────────────────

def get_attached_volume_ids(ec2_client):
    """
    Return a set of all EBS volume IDs currently attached to any
    EC2 instance — running OR stopped.

    We include stopped instances because their volumes are still
    "in use". Deleting snapshots for them could break instance recovery.
    """
    attached_volume_ids = set()

    paginator = ec2_client.get_paginator("describe_instances")

    for page in paginator.paginate(
        Filters=[{
            "Name":   "instance-state-name",
            "Values": ["running", "stopped", "stopping", "pending"],
        }]
    ):
        for reservation in page["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]

                for mapping in instance.get("BlockDeviceMappings", []):
                    vol_id = mapping.get("Ebs", {}).get("VolumeId")
                    if vol_id:
                        attached_volume_ids.add(vol_id)
                        logger.debug(
                            "Volume %s is attached to instance %s",
                            vol_id, instance_id
                        )

    logger.info(
        "Found %d volumes attached to EC2 instances",
        len(attached_volume_ids)
    )
    return attached_volume_ids


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Get all old snapshots
# ─────────────────────────────────────────────────────────────────────────────

def get_old_snapshots(ec2_client, retention_days):
    """
    Return all snapshots owned by this account that are older
    than retention_days.
    """
    cutoff    = datetime.now(timezone.utc) - timedelta(days=retention_days)
    paginator = ec2_client.get_paginator("describe_snapshots")

    old_snapshots = []
    total_scanned = 0

    for page in paginator.paginate(OwnerIds=["self"]):
        for snap in page["Snapshots"]:
            total_scanned += 1
            if snap["StartTime"] < cutoff:
                old_snapshots.append(snap)

    logger.info(
        "Scanned %d total snapshots — %d are older than %d days",
        total_scanned, len(old_snapshots), retention_days
    )
    return old_snapshots, total_scanned


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Delete a single snapshot safely
# ─────────────────────────────────────────────────────────────────────────────

def delete_snapshot_safe(ec2_client, snapshot_id, dry_run=True):
    """
    Delete a snapshot. Returns True on success, False on expected errors.

    Expected (skip silently):
        InvalidSnapshot.InUse   — snapshot is being used to create AMI/volume
        InvalidSnapshot.NotFound — already deleted (race condition)

    Unexpected (re-raises):
        UnauthorizedOperation   — IAM permissions problem
        Any other ClientError   — should not happen, investigate
    """
    if dry_run:
        logger.info("[DRY RUN] Would delete: %s", snapshot_id)
        return True

    try:
        ec2_client.delete_snapshot(SnapshotId=snapshot_id)
        logger.info("Deleted snapshot: %s", snapshot_id)
        return True

    except ClientError as e:
        code    = e.response["Error"]["Code"]
        message = e.response["Error"]["Message"]

        if code == "InvalidSnapshot.InUse":
            logger.warning(
                "SKIP %s — currently in use (being used to create an AMI or volume)",
                snapshot_id
            )
            return False

        elif code == "InvalidSnapshot.NotFound":
            logger.warning(
                "SKIP %s — not found (may have been deleted already)",
                snapshot_id
            )
            return False

        elif code == "UnauthorizedOperation":
            logger.error(
                "PERMISSION DENIED deleting %s — check Lambda execution role",
                snapshot_id
            )
            raise

        else:
            logger.error(
                "Unexpected error deleting %s | code=%s | message=%s",
                snapshot_id, code, message
            )
            raise


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Publish SNS report
# ─────────────────────────────────────────────────────────────────────────────

def publish_report(sns_client, topic_arn, report):
    """Publish the cleanup summary to an SNS topic."""
    if not topic_arn:
        logger.info("SNS_TOPIC_ARN not set — skipping notification")
        return

    # Build message body
    lines = [
        "EBS SNAPSHOT CLEANUP REPORT",
        "=" * 45,
        f"Run date         : {report['run_date']}",
        f"Region           : {report['region']}",
        f"Retention policy : {report['retention_days']} days",
        f"Mode             : {'DRY RUN (no deletes)' if report['dry_run'] else 'LIVE'}",
        "",
        f"Snapshots scanned    : {report['total_scanned']}",
        f"Older than {report['retention_days']} days : {report['old_count']}",
        f"Eligible for delete  : {report['eligible']}",
        f"Successfully deleted : {report['deleted']}",
        f"Skipped (in use)     : {report['skipped']}",
        f"Errors               : {report['errors']}",
        f"Estimated freed      : {report['freed_gb']:.1f} GB",
        "",
    ]

    if report["deleted_snapshots"]:
        lines.append("Deleted snapshots (up to 20 shown):")
        for snap in report["deleted_snapshots"][:20]:
            lines.append(
                f"  {snap['id']:<25}  "
                f"vol: {snap['volume_id']:<25}  "
                f"{snap['size_gb']:>4} GB  "
                f"{snap['age_days']:>3}d old  "
                f"{snap['name']}"
            )
        remaining = len(report["deleted_snapshots"]) - 20
        if remaining > 0:
            lines.append(f"  ... and {remaining} more")

    message = "\n".join(lines)

    subject = (
        f"[{'DRY RUN — ' if report['dry_run'] else ''}Snapshot Cleanup] "
        f"{report['deleted']} deleted | "
        f"{report['freed_gb']:.1f} GB freed | "
        f"{report['region']}"
    )

    try:
        sns_client.publish(
            TopicArn = topic_arn,
            Subject  = subject,
            Message  = message,
        )
        logger.info("SNS report published to %s", topic_arn)

    except ClientError as e:
        logger.error("Failed to publish SNS report: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Main Lambda entry point.

    Triggered by:
        - EventBridge scheduled rule (every Sunday midnight UTC)
        - Manual invoke for testing

    Args:
        event   : dict from EventBridge (unused — no payload needed)
        context : Lambda context (has remaining_time_in_millis, function_name)

    Returns:
        dict with statusCode and summary body
    """
    logger.info(
        "=== Snapshot Cleanup Started | region=%s | retention=%dd | dry_run=%s ===",
        REGION, RETENTION_DAYS, DRY_RUN
    )

    if DRY_RUN:
        logger.info(
            "*** DRY RUN MODE — no snapshots will be deleted. "
            "Set DRY_RUN=false to enable live deletions. ***"
        )

    # Initialise AWS clients
    ec2 = boto3.client("ec2", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)

    # ── Step 1: Get all volumes attached to instances ─────────────────────────
    try:
        attached_volume_ids = get_attached_volume_ids(ec2)
    except Exception as e:
        logger.error("FATAL: Could not retrieve attached volumes: %s", e)
        raise

    # ── Step 2: Get all snapshots older than RETENTION_DAYS ───────────────────
    try:
        old_snapshots, total_scanned = get_old_snapshots(ec2, RETENTION_DAYS)
    except Exception as e:
        logger.error("FATAL: Could not retrieve old snapshots: %s", e)
        raise

    # ── Step 3: Filter — snapshots whose source volume is NOT attached ─────────
    #
    # A snapshot is a candidate for deletion if:
    #   a) It is older than RETENTION_DAYS  (already filtered above)
    #   b) Its source volume is not attached to any EC2 instance
    #
    # This protects snapshots of volumes that are actively in use.
    #
    candidates = [
        snap for snap in old_snapshots
        if snap.get("VolumeId") not in attached_volume_ids
    ]

    logger.info(
        "%d of %d old snapshots are candidates "
        "(source volume not attached to any instance)",
        len(candidates), len(old_snapshots)
    )

    # ── Step 4: Delete eligible snapshots ────────────────────────────────────
    report = {
        "run_date":          datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "region":            REGION,
        "retention_days":    RETENTION_DAYS,
        "dry_run":           DRY_RUN,
        "total_scanned":     total_scanned,
        "old_count":         len(old_snapshots),
        "eligible":          len(candidates),
        "deleted":           0,
        "skipped":           0,
        "errors":            0,
        "freed_gb":          0.0,
        "deleted_snapshots": [],
    }

    for snap in candidates:
        snapshot_id = snap["SnapshotId"]
        volume_id   = snap.get("VolumeId", "vol-unknown")
        size_gb     = snap.get("VolumeSize", 0)
        age_days    = (datetime.now(timezone.utc) - snap["StartTime"]).days
        name        = next(
            (t["Value"] for t in snap.get("Tags", []) if t["Key"] == "Name"),
            snapshot_id
        )

        try:
            success = delete_snapshot_safe(ec2, snapshot_id, dry_run=DRY_RUN)

            if success:
                report["deleted"]  += 1
                report["freed_gb"] += size_gb
                report["deleted_snapshots"].append({
                    "id":        snapshot_id,
                    "volume_id": volume_id,
                    "size_gb":   size_gb,
                    "age_days":  age_days,
                    "name":      name,
                })
            else:
                report["skipped"] += 1

        except Exception as e:
            report["errors"] += 1
            logger.error("Error processing snapshot %s: %s", snapshot_id, e)
            # Do NOT raise — continue processing remaining snapshots

    # ── Step 5: Publish report ─────────────────────────────────────────────────
    logger.info(
        "=== Cleanup Complete | deleted=%d | skipped=%d | errors=%d | freed=%.1f GB ===",
        report["deleted"], report["skipped"],
        report["errors"],  report["freed_gb"]
    )

    publish_report(sns, SNS_TOPIC_ARN, report)

    return {
        "statusCode": 200,
        "body": {
            "message":       "Snapshot cleanup complete",
            "dry_run":       DRY_RUN,
            "total_scanned": total_scanned,
            "deleted":       report["deleted"],
            "skipped":       report["skipped"],
            "errors":        report["errors"],
            "freed_gb":      round(report["freed_gb"], 2),
        }
    }
