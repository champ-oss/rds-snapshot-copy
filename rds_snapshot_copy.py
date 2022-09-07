#!/usr/bin/python

from __future__ import annotations

import logging
import time
import os
import boto3

rds = boto3.client('rds')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# delete latest snapshot
def delete_latest_snapshot(snapshot: str) -> None:
    logger.info("delete latest snapshot")
    rds.delete_db_snapshot(
        DBSnapshotIdentifier=snapshot,
    )
    time.sleep(5)
    logger.info("deleted latest snapshot complete")


# copy snapshot
def copy_snapshot(source_snapshot: str, target_snapshot: str, kms_key_id: str) -> None:
    logger.info("copy snapshot")

    rds.copy_db_snapshot(
        SourceDBSnapshotIdentifier=source_snapshot,
        TargetDBSnapshotIdentifier=target_snapshot,
        KmsKeyId=kms_key_id,
    )
    logger.info("copy snapshot complete")


def lambda_handler(event, context):
    # env variables requirement, used to share snapshot with another account
    kms_key_id = os.environ['KMS_KEY_ID']

    # get list of manual snapshots and delete
    db_snapshot_list = list()
    response = rds.describe_db_snapshots(SnapshotType='manual')
    for i in response['DBSnapshots']:
        db_snapshot_name = i['DBSnapshotIdentifier']
        db_snapshot_list.append(db_snapshot_name)
    for db_snapshot in db_snapshot_list:
        print(db_snapshot)
        delete_latest_snapshot(db_snapshot)
    db_snapshot_list.clear()

    # get a list of shared snapshots and copy
    db_snapshot_shared_list = list()
    response = rds.describe_db_snapshots(
        SnapshotType='shared',
        IncludeShared=True)
    for i in response['DBSnapshots']:
        db_shared_snapshot_name = i['DBSnapshotIdentifier']
        db_snapshot_shared_list.append(db_shared_snapshot_name)
    for db_snapshot_shared_instance in db_snapshot_shared_list:
        target_snapshot = db_snapshot_shared_instance.split(":")[6]
        copy_snapshot(db_snapshot_shared_instance,target_snapshot,kms_key_id)
    db_snapshot_shared_list.clear()
