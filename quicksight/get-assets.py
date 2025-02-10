import json
from datetime import date, datetime
from functools import partial
from operator import itemgetter
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Iterator, Literal, Mapping, Sequence

import boto3
import httpx
import typer
from dotenv import load_dotenv
from icecream import ic
from loguru import logger
from mypy_boto3_quicksight import QuickSightClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
ROOT_DIR = Path(__file__).parent.absolute()
QS_EXPORT_DIR = ROOT_DIR.joinpath("data")

app = typer.Typer()

EXPORT_JOB_NAME = "quicksight-export"
IMPORT_JOB_NAME = "quicksight-import"

type_getter = itemgetter("Type")
name_getter = itemgetter("Name")
arn_getter = itemgetter("Arn")


def get_qs_all_assets(qs_client: QuickSightClient) -> Mapping:
    """Retrieve all QuickSight assets from the given region."""

    def filter_successful(assets: Iterable) -> Iterable:
        successful_assets = []
        for asset in assets:
            asset_status = asset.get("Status", "")
            if not asset_status:
                successful_assets.append(asset)
                continue
            if asset_status in ("CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"):
                successful_assets.append(asset)
        return successful_assets

    return {
        "data_sources": filter_successful(
            qs_client.list_data_sources(AwsAccountId=AWS_ACCOUNT_ID).get(
                "DataSources", []
            )
        ),
        "data_sets": qs_client.list_data_sets(AwsAccountId=AWS_ACCOUNT_ID).get(
            "DataSetSummaries", []
        ),
        "analyses": filter_successful(
            qs_client.list_analyses(AwsAccountId=AWS_ACCOUNT_ID).get(
                "AnalysisSummaryList", []
            )
        ),
        "dashboards": qs_client.list_dashboards(AwsAccountId=AWS_ACCOUNT_ID).get(
            "DashboardSummaryList", []
        ),
        "folders": qs_client.list_folders(AwsAccountId=AWS_ACCOUNT_ID).get(
            "FolderSummaryList", []
        ),
    }


def get_qs_refresh_schedules(
    qs_client: QuickSightClient, dataset_id: str
) -> Sequence[Mapping[str, Any]]:
    return qs_client.list_refresh_schedules(
        AwsAccountId=AWS_ACCOUNT_ID, DataSetId=dataset_id
    ).get("RefreshSchedules", [])


def get_qs_dataset_refresh_schedules(
    qs_client: QuickSightClient, dataset_id_list: Sequence
) -> Mapping[str, Sequence[Mapping[str, Any]]]:
    """Retrieve all Glue tables for a list of databases."""
    return {
        dataset_id: get_qs_refresh_schedules(qs_client, dataset_id)
        for dataset_id in dataset_id_list
    }


def get_qs_folder_ids(qs_client: QuickSightClient) -> Iterable[str]:
    return map(
        itemgetter("FolderId"),
        qs_client.list_folders(AwsAccountId=AWS_ACCOUNT_ID).get(
            "FolderSummaryList", []
        ),
    )


def get_qs_folder_with_permission(qs_client: QuickSightClient, folder_id):
    folder = qs_client.describe_folder(
        AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_id
    ).get("Folder", [])
    permissions = qs_client.describe_folder_resolved_permissions(
        AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_id
    ).get("Permissions", [])
    return {**folder, "Permissions": permissions}


def get_qs_folders_sorted(
    qs_client: QuickSightClient, reverse: bool = False
) -> Iterable:
    folder_ids = get_qs_folder_ids(qs_client)
    qs_folders = map(
        partial(get_qs_folder_with_permission, qs_client),
        folder_ids,
    )
    return sorted(
        qs_folders, key=lambda x: len(x.get("FolderPath", [])), reverse=reverse
    )


def get_keys(data_sources, func_getter, criteria):
    return list(map(func_getter, filter(criteria, data_sources)))


def get_arns(data_sources, criteria):
    return get_keys(data_sources, arn_getter, criteria)


@app.command()
def main(region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    qs_client = session.client("quicksight", region_name=region)

    # Get assets
    assets = get_qs_all_assets(qs_client)

    for k, v in assets.items():
        ic(k)
        ic(v)

    logger.info("Quicksight migration completed successfully")


if __name__ == "__main__":
    app()
