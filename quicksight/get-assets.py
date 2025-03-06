from functools import partial
from operator import itemgetter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import boto3
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


def filter_successful(assets: Iterable) -> Iterable:
    return list(
        filter(
            lambda x: x["Status"] in ("CREATION_SUCCESSFUL", "UPDATE_SUCCESSFUL"),
            assets,
        )
    )


def get_qs_paginated_assets(
    qs_client: QuickSightClient, method_name: str, key: str
) -> Iterable:
    """Helper function to paginate through QuickSight API responses."""
    paginator = qs_client.get_paginator(method_name)
    for page in paginator.paginate(AwsAccountId=AWS_ACCOUNT_ID):
        for asset in page.get(key, []):
            yield asset


def get_qs_data_sources(qs_client: QuickSightClient) -> Iterable:
    """Retrieve QuickSight data sources with pagination support."""

    def filter_data_sources(assets: Iterable) -> Iterable:
        return list(filter(lambda x: "DataSourceParameters" in x, assets))

    return filter_data_sources(
        get_qs_paginated_assets(qs_client, "list_data_sources", "DataSources")
    )


def get_qs_data_sets(qs_client: QuickSightClient) -> Iterable:
    """Retrieve QuickSight data sets with pagination support."""
    return get_qs_paginated_assets(qs_client, "list_data_sets", "DataSetSummaries")


def get_qs_analyses(qs_client: QuickSightClient) -> Iterable:
    """Retrieve QuickSight analyses with pagination support."""
    return filter_successful(
        get_qs_paginated_assets(qs_client, "list_analyses", "AnalysisSummaryList")
    )


def get_qs_dashboards(qs_client: QuickSightClient) -> Iterable:
    """Retrieve QuickSight dashboards with pagination support."""
    return get_qs_paginated_assets(qs_client, "list_dashboards", "DashboardSummaryList")


def get_qs_folders(qs_client: QuickSightClient) -> Iterable:
    """Retrieve QuickSight folders with pagination support."""
    return get_qs_paginated_assets(qs_client, "list_folders", "FolderSummaryList")


def get_qs_all_assets(qs_client: QuickSightClient) -> Mapping:
    """Retrieve all QuickSight assets from the given region."""

    data_sources = get_qs_data_sources(qs_client)
    data_sets = get_qs_data_sets(qs_client)
    analyses = get_qs_analyses(qs_client)
    dashboards = get_qs_dashboards(qs_client)
    folders = get_qs_folders(qs_client)

    return {
        "data_sources": data_sources,
        "data_sets": data_sets,
        "analyses": analyses,
        "dashboards": dashboards,
        "folders": folders,
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
    logger.info("Fetching QuickSight assets...")
    assets = get_qs_all_assets(qs_client)

    QS_EXPORT_DIR.mkdir(exist_ok=True)
    with open(QS_EXPORT_DIR.joinpath(f"qs_list_assets-{region}.csv"), "w") as f:
        f.write("asset_type,name,arn")
        f.write("\n")
        for k, v in assets.items():
            if not v:
                continue
            for _ in v:
                f.write(f"{k},{_['Name']},{_['Arn']}")
                f.write("\n")


if __name__ == "__main__":
    app()
