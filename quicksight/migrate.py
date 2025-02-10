from operator import itemgetter
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence

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

app = typer.Typer()

EXPORT_JOB_NAME = "quicksight-export"
IMPORT_JOB_NAME = "quicksight-import"

datasource_name = "data-test12345"
dataset_name = "netflix_data"
analysis_name = "netflix_data analysis-test12345"
dashboard_name = "netflix_data_dashboard-test12345"
asset_export_job_name = "test-1"
asset_import_job_name = "test-1"

type_getter = itemgetter("Type")
name_getter = itemgetter("Name")
arn_getter = itemgetter("Arn")


def get_qs_all_assets(qs_client: QuickSightClient) -> Dict:
    """Retrieve all QuickSight assets from the given region."""
    return {
        "data_sources": qs_client.list_data_sources(AwsAccountId=AWS_ACCOUNT_ID).get(
            "DataSources", []
        ),
        "data_sets": qs_client.list_data_sets(AwsAccountId=AWS_ACCOUNT_ID).get(
            "DataSetSummaries", []
        ),
        "analyses": qs_client.list_analyses(AwsAccountId=AWS_ACCOUNT_ID).get(
            "AnalysisSummaryList", []
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
) -> Dict[str, Sequence[Mapping[str, Any]]]:
    """Retrieve all Glue tables for a list of databases."""
    return {
        dataset_id: get_qs_refresh_schedules(qs_client, dataset_id)
        for dataset_id in dataset_id_list
    }


def export_assets(qs_client: QuickSightClient, resource_arns: Sequence[str]) -> bytes:
    """Export all QuickSight assets to a downloadable bundle."""
    logger.info("Starting asset export...")
    response = qs_client.start_asset_bundle_export_job(
        AwsAccountId=AWS_ACCOUNT_ID,
        AssetBundleExportJobId=EXPORT_JOB_NAME,
        ResourceArns=resource_arns,
        IncludeAllDependencies=True,
        ExportFormat="QUICKSIGHT_JSON",
        IncludePermissions=True,
        IncludeFolderMemberships=True,
        IncludeFolderMembers="RECURSE",
        IncludeTags=True,
    )
    job_id = response["AssetBundleExportJobId"]

    # Polling export job status
    while True:
        job_status = qs_client.describe_asset_bundle_export_job(
            AwsAccountId=AWS_ACCOUNT_ID, AssetBundleExportJobId=job_id
        )
        if job_status["JobStatus"] in ["SUCCESSFUL", "FAILED"]:
            break
        sleep(2)

    if job_status["JobStatus"] == "FAILED":
        raise Exception("Quicksight asset export failed")

    # Download asset bundle
    asset_bundle_url = job_status.get("DownloadUrl", "")
    if not asset_bundle_url:
        raise Exception("Quicksight asset export failed")
    with httpx.Client() as client:
        r = client.get(asset_bundle_url)
    return r.content


def import_assets(qs_client: QuickSightClient, asset_data: bytes):
    """Import QuickSight assets from a downloaded bundle."""
    logger.info("Starting asset import...")
    response = qs_client.start_asset_bundle_import_job(
        AwsAccountId=AWS_ACCOUNT_ID,
        AssetBundleImportJobId=IMPORT_JOB_NAME,
        AssetBundleImportSource={"Body": asset_data},
        FailureAction="ROLLBACK",
    )
    job_id = response["AssetBundleImportJobId"]

    # Polling import job status
    while True:
        job_status = qs_client.describe_asset_bundle_import_job(
            AwsAccountId=AWS_ACCOUNT_ID, AssetBundleImportJobId=job_id
        )
        if job_status["JobStatus"] in [
            "SUCCESSFUL",
            "FAILED",
            "FAILED_ROLLBACK_COMPLETED",
        ]:
            break
        sleep(2)

    if job_status["JobStatus"] in ["FAILED", "FAILED_ROLLBACK_COMPLETED"]:
        raise Exception("Quicksight asset import failed")

    logger.info("Quicksight asset import completed successfully")


def migrate_folders_and_permissions(
    qs_source: QuickSightClient, qs_target: QuickSightClient
):
    """Migrate QuickSight folders and their permissions."""
    logger.info("Migrating folders and permissions...")
    source_folders = qs_source.list_folders(AwsAccountId=AWS_ACCOUNT_ID).get(
        "FolderSummaryList", []
    )

    for folder in source_folders:
        folder_name = folder.get("Name", "")
        folder_id = folder.get("FolderId", "")
        if not folder_name:
            continue
        try:
            qs_target.create_folder(
                AwsAccountId=AWS_ACCOUNT_ID,
                FolderId=folder_id,
                Name=folder_name,
                FolderType=folder.get("FolderType", "RESTRICTED"),
            )
        except (Exception, qs_target.exceptions.ResourceExistsException) as e:
            logger.warning(f"Folder {folder_name} already axists: {e}")

        # Copy folder permissions
        permissions = qs_source.describe_folder_permissions(
            AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_id
        )
        qs_target.update_folder_permissions(
            AwsAccountId=AWS_ACCOUNT_ID,
            FolderId=folder_id,
            GrantPermissions=permissions.get("Permissions", []),
        )

    logger.info("Folders and permissions migrated successfully.")


def get_keys(data_sources, func_getter, criteria):
    return list(map(func_getter, filter(criteria, data_sources)))


def get_arns(data_sources, criteria):
    return get_keys(data_sources, arn_getter, criteria)


def data_source_criteria(data_source):
    return (
        type_getter(data_source) == "ATHENA"
        and name_getter(data_source) == datasource_name
    )


def data_set_criteria(data_set):
    return (
        itemgetter("ImportMode")(data_set) == "SPICE"
        and name_getter(data_set) == dataset_name
    )


def analysis_criteria(analysis):
    return (
        # itemgetter("ImportMode")(analysis) == "SPICE"
        name_getter(analysis) == analysis_name
    )


def dashboard_criteria(dashboard):
    return name_getter(dashboard) == dashboard_name


@app.command()
def main(source_region: str, target_region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    qs_source = session.client("quicksight", region_name=source_region)
    qs_target = session.client("quicksight", region_name=target_region)

    # Get assets
    source_assets = get_qs_all_assets(qs_source)
    target_assets = get_qs_all_assets(qs_target)

    source_data_sources = source_assets["data_source"]
    source_data_sets = source_assets["data_sets"]
    source_analysis = source_assets["analysis"]
    source_dashboards = source_assets["dashboards"]

    target_data_sources = target_assets["data_source"]
    target_data_sets = target_assets["data_sets"]
    target_analysis = target_assets["analysis"]
    target_dashboards = target_assets["dashboards"]

    source_data_source_arns = get_arns(source_data_sources, lambda x: x)
    source_data_set_arns = get_arns(source_data_sets, lambda x: x)
    source_analysis_arns = get_arns(source_analysis, lambda x: x)
    source_dashboard_arns = get_arns(source_dashboards, lambda x: x)

    source_data_source_ids = get_keys(
        source_data_sources, itemgetter("DataSourceId"), lambda x: x
    )
    source_data_set_ids = get_keys(
        source_data_sets, itemgetter("DataSetId"), lambda x: x
    )
    source_analysis_ids = get_keys(
        source_analysis, itemgetter("AnalysisId"), lambda x: x
    )
    source_dashboard_ids = get_keys(
        source_dashboards, itemgetter("DashboardId"), lambda x: x
    )

    target_data_source_arns = get_arns(target_data_sources, lambda x: x)
    target_data_set_arns = get_arns(target_data_sets, lambda x: x)
    target_analysis_arns = get_arns(target_analysis, lambda x: x)
    target_dashboard_arns = get_arns(target_dashboards, lambda x: x)

    target_data_source_ids = get_keys(
        target_data_sources, itemgetter("DataSourceId"), lambda x: x
    )
    target_data_set_ids = get_keys(
        target_data_sets, itemgetter("DataSetId"), lambda x: x
    )
    target_analysis_ids = get_keys(
        target_analysis, itemgetter("AnalysisId"), lambda x: x
    )
    target_dashboard_ids = get_keys(
        target_dashboards, itemgetter("DashboardId"), lambda x: x
    )

    ic(source_data_source_arns)
    ic(source_data_set_arns)
    ic(source_analysis_arns)
    ic(source_dashboard_arns)

    ic(source_data_source_ids)
    ic(source_data_set_ids)
    ic(source_analysis_ids)
    ic(source_dashboard_ids)

    ic(target_data_source_arns)
    ic(target_data_set_arns)
    ic(target_analysis_arns)
    ic(target_dashboard_arns)

    ic(target_data_source_ids)
    ic(target_data_set_ids)
    ic(target_analysis_ids)
    ic(target_dashboard_ids)

    # Export assets
    asset_data = export_assets(
        qs_source,
        [
            *source_dashboard_arns,
            *source_analysis_arns,
            *source_data_set_arns,
            *source_data_source_arns,
        ],
    )
    with open(ROOT_DIR.joinpath("data/quicksight_asset_bundle.qs"), "wb") as f:
        f.write(asset_data)

    # Import assets
    import_assets(qs_target, asset_data)

    # Migrate folders and permissions
    migrate_folders_and_permissions(qs_source, qs_target)

    logger.info("Quicksight migration completed successfully")


if __name__ == "__main__":
    app()
