from operator import itemgetter
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Iterator, List, Literal, Mapping, Sequence

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


def get_qs_all_assets(qs_client: QuickSightClient) -> Dict:
    """Retrieve all QuickSight assets from the given region."""

    def filter_successful(assets: Iterable) -> Iterable:
        successful_assets = []
        for asset in assets:
            asset_status = asset.get("status", "")
            if asset_status:
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


def get_qs_folder_ids(qs_client: QuickSightClient) -> Iterable[str]:
    return map(
        itemgetter("FolderId"),
        qs_client.list_folders(AwsAccountId=AWS_ACCOUNT_ID).get(
            "FolderSummaryList", []
        ),
    )


def get_qs_folders_sorted(
    qs_client: QuickSightClient, reverse: bool = False
) -> Iterable:
    folder_ids = get_qs_folder_ids(qs_client)
    qs_folders = map(
        lambda x: qs_client.describe_folder(
            AwsAccountId=AWS_ACCOUNT_ID, FolderId=x
        ).get("Folder", []),
        folder_ids,
    )
    return sorted(
        qs_folders, key=lambda x: len(x.get("FolderPath", [])), reverse=reverse
    )


def migrate_folders_and_members(
    qs_source: QuickSightClient, qs_target: QuickSightClient
):
    """Migrate QuickSight folders and their permissions."""

    def get_member_type_from_arn(
        arn: str,
    ) -> Literal["DASHBOARD", "ANALYSIS", "DATASET", "DATASOURCE", "TOPIC"]:
        return arn.split(":")[-1].split("/")[0].upper()  # type: ignore

    logger.info("Migrating folders and permissions...")

    qs_folders = get_qs_folders_sorted(qs_source)

    for folder in qs_folders:
        folder_id = folder.get("FolderId", "")
        folder_name = folder.get("Name", "")
        folder_path = folder.get("FolderPath", [])
        folder_parent_arn = folder_path[-1] if folder_path else ""

        # Copy folder permissions
        permissions = qs_source.describe_folder_resolved_permissions(
            AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_id
        ).get("Permissions", [])
        folder_members = qs_source.list_folder_members(
            AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_id
        ).get("FolderMemberList", [])

        try:
            qs_target.create_folder(
                AwsAccountId=AWS_ACCOUNT_ID,
                FolderId=folder_id,
                Name=folder_name,
                FolderType=folder.get("FolderType", "RESTRICTED"),
                ParentFolderArn=folder_parent_arn,
                Permissions=permissions,
            )
        except (Exception, qs_target.exceptions.ResourceExistsException) as e:
            logger.warning(f"Folder {folder_name} already exists: {e}")

        for folder_member in folder_members:
            member_id = folder_member.get("MemberId", "")
            member_arn = folder_member.get("MemberArn", "")
            member_type = get_member_type_from_arn(member_arn)

            try:
                qs_target.create_folder_membership(
                    AwsAccountId=AWS_ACCOUNT_ID,
                    FolderId=folder_id,
                    MemberId=member_id,
                    MemberType=member_type,
                )
            except (Exception, qs_target.exceptions.ResourceExistsException) as e:
                logger.warning(
                    f"Member {member_id} already exists in folder {folder_name}: {e}"
                )

    logger.info("Folders and permissions migrated successfully.")


def get_keys(data_sources, func_getter, criteria):
    return list(map(func_getter, filter(criteria, data_sources)))


def get_arns(data_sources, criteria):
    return get_keys(data_sources, arn_getter, criteria)


@app.command()
def main(source_region: str, target_region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    qs_source = session.client("quicksight", region_name=source_region)
    qs_target = session.client("quicksight", region_name=target_region)

    # Get assets
    source_assets = get_qs_all_assets(qs_source)

    source_data_sources = source_assets["data_sources"]
    source_data_sets = source_assets["data_sets"]
    source_analyses = source_assets["analyses"]
    source_dashboards = source_assets["dashboards"]

    source_data_source_arns = get_arns(source_data_sources, lambda x: x)
    source_data_set_arns = get_arns(source_data_sets, lambda x: x)
    source_analyses_arns = get_arns(source_analyses, lambda x: x)
    source_dashboard_arns = get_arns(source_dashboards, lambda x: x)

    # Export assets
    asset_data = export_assets(
        qs_source,
        [
            *source_dashboard_arns,
            *source_analyses_arns,
            *source_data_set_arns,
            *source_data_source_arns,
        ],
    )
    QS_EXPORT_DIR.mkdir(exist_ok=True)
    with open(QS_EXPORT_DIR.joinpath("quicksight_asset_bundle.qs"), "wb") as f:
        f.write(asset_data)

    # Import assets
    import_assets(qs_target, asset_data)

    # Migrate folders and permissions
    migrate_folders_and_members(qs_source, qs_target)

    logger.info("Quicksight migration completed successfully")


if __name__ == "__main__":
    app()
