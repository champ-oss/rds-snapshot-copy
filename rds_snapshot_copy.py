#!/usr/bin/python

from __future__ import annotations

import logging
import time
import os
from typing import List

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


def delete_cluster_latest_snapshot(latest_snapshot: str) -> None:
    logger.info("delete latest snapshot")
    rds.delete_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=latest_snapshot,
    )
    time.sleep(5)
    logger.info("deleted latest cluster snapshot complete")


# copy snapshot
def copy_snapshot(source_snapshot: str, target_snapshot: str, kms_key_id: str) -> None:
    logger.info("copy snapshot")

    rds.copy_db_snapshot(
        SourceDBSnapshotIdentifier=source_snapshot,
        TargetDBSnapshotIdentifier=target_snapshot,
        KmsKeyId=kms_key_id,
    )
    logger.info("copy snapshot complete")


def copy_cluster_snapshot(source_snapshot: str, target_snapshot: str, kms_key_id: str) -> None:
    logger.info("copy cluster snapshot")

    rds.copy_db_cluster_snapshot(
        SourceDBClusterSnapshotIdentifier=source_snapshot,
        TargetDBClusterSnapshotIdentifier=target_snapshot,
        KmsKeyId=kms_key_id,
    )
    logger.info("copy cluster snapshot complete")


# get latest manual snapshots
def get_latest_manual_snapshot() -> List[str] | None:
    logger.info(f"get latest snapshot")
    try:
        db_snapshot_list = list()
        response = rds.describe_db_snapshots(SnapshotType='manual')
        for i in response['DBSnapshots']:
            db_snapshot_name = i['DBSnapshotIdentifier']
            db_snapshot_list.append(db_snapshot_name)
        logger.info("getting db instances complete")
        return db_snapshot_list
    except (Exception,):
        return


def get_latest_manual_cluster_snapshot() -> List[str] | None:
    logger.info(f"get latest cluster snapshot")
    try:
        db_cluster_snapshot_list = list()
        response = rds.describe_db_cluster_snapshots(SnapshotType='manual')
        for i in response['DBSnapshots']:
            db_cluster_snapshot_name = i['DBClusterSnapshotIdentifier']
            db_cluster_snapshot_list.append(db_cluster_snapshot_name)
        logger.info("getting db cluster complete")
        return db_cluster_snapshot_list
    except (Exception,):
        return


def lambda_handler(event, context):
    # env variables requirement, used to share snapshot with another account
    kms_key_id = os.environ['KMS_KEY_ID']

    # get list of manual snapshots and delete
    db_snapshot_list = get_latest_manual_snapshot()
    for db_snapshot in db_snapshot_list:
        delete_latest_snapshot(db_snapshot)

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
        copy_snapshot(db_snapshot_shared_instance, target_snapshot, kms_key_id)
    db_snapshot_shared_list.clear()

    # get list of manual cluster snapshots and delete
    db_cluster_snapshot_list = get_latest_manual_cluster_snapshot()
    for db_cluster_snapshot in db_cluster_snapshot_list:
        delete_cluster_latest_snapshot(db_cluster_snapshot)

    # get a list of shared cluster snapshots and copy
    db_cluster_snapshot_shared_list = list()
    cluster_response = rds.describe_db_snapshots(
        SnapshotType='shared',
        IncludeShared=True)
    for i in cluster_response['DBSnapshots']:
        db_cluster_snapshot_shared_name = i['DBClusterSnapshotIdentifier']
        db_cluster_snapshot_shared_list.append(db_cluster_snapshot_shared_name)
    for db_snapshot_shared_cluster in db_cluster_snapshot_shared_list:
        target_cluster_snapshot = db_snapshot_shared_cluster.split(":")[6]
        copy_cluster_snapshot(db_snapshot_shared_cluster, target_cluster_snapshot, kms_key_id)
    db_cluster_snapshot_shared_list.clear()

