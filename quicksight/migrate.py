from datetime import datetime
from functools import partial
from itertools import batched
from operator import itemgetter
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Literal, Mapping, Sequence

import boto3
import httpx
import typer
from dotenv import load_dotenv
from loguru import logger
from mypy_boto3_quicksight import QuickSightClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
ROOT_DIR = Path(__file__).parent.absolute()
LOG_DIR = ROOT_DIR.joinpath("log")
QS_EXPORT_DIR = ROOT_DIR.joinpath("data")
QS_RETRY_DIR = QS_EXPORT_DIR.joinpath("retry")

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
        logger.error(f"{job_status['Errors']}. Saved failed arns to retry dir")
        with open(
            QS_RETRY_DIR.joinpath(f"arns-{datetime.now().timestamp()}.txt", "w")
        ) as f:
            f.writelines(job_status["ResourceArns"])
        raise Exception("QuickSight asset export failed")

    # Download asset bundle
    asset_bundle_url = job_status.get("DownloadUrl", "")
    if not asset_bundle_url:
        raise Exception("QuickSight asset export failed")
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
        logger.error(job_status["Errors"])
        raise Exception("QuickSight asset import failed")

    logger.info("QuickSight asset import completed successfully")


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


def create_qs_folder_helper(qs_client: QuickSightClient, folder):
    def get_id_from_arn(arn: str) -> str:
        return arn.split(":")[-1].split("/")[-1]

    folder_id = folder.get("FolderId", "")
    folder_name = folder.get("Name", "")
    folder_type = folder.get("FolderType", "RESTRICTED")
    permissions = folder.get("Permissions", [])
    if folder_path := folder.get("FolderPath", []):
        folder_parent_arn = folder_path[-1] if folder_path else ""
        folder_parent_id = get_id_from_arn(folder_parent_arn)
        folder_parent_arn = (
            qs_client.describe_folder(
                AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_parent_id
            )
            .get("Folder", {})
            .get("Arn", "")
        )
        qs_client.create_folder(
            AwsAccountId=AWS_ACCOUNT_ID,
            FolderId=folder_id,
            Name=folder_name,
            FolderType=folder_type,
            ParentFolderArn=folder_parent_arn,
            Permissions=permissions,
        )
    else:
        qs_client.create_folder(
            AwsAccountId=AWS_ACCOUNT_ID,
            FolderId=folder_id,
            Name=folder_name,
            FolderType=folder_type,
            Permissions=permissions,
        )


def migrate_folders_and_members(
    qs_source: QuickSightClient, qs_target: QuickSightClient
):
    """Migrate QuickSight folders and their permissions."""

    def get_member_type_from_arn(
        arn: str,
    ) -> Literal["DASHBOARD", "ANALYSIS", "DATASET", "DATASOURCE", "TOPIC"]:
        return arn.split(":")[-1].split("/")[0].upper()  # type: ignore

    logger.info("Migrating folders and members...")

    qs_folders = get_qs_folders_sorted(qs_source)

    for folder in qs_folders:
        folder_id = folder.get("FolderId", "")
        folder_name = folder.get("Name", "")

        folder_members = qs_source.list_folder_members(
            AwsAccountId=AWS_ACCOUNT_ID, FolderId=folder_id
        ).get("FolderMemberList", [])

        try:
            logger.info(f"Creating folder: {folder_name}")
            create_qs_folder_helper(qs_target, folder)
        except qs_target.exceptions.ResourceExistsException as e:
            logger.warning(f"Folder {folder_name} already exists: {e}")

        for folder_member in folder_members:
            member_id = folder_member.get("MemberId", "")
            member_arn = folder_member.get("MemberArn", "")
            member_type = get_member_type_from_arn(member_arn)

            try:
                logger.info(f"Creating member: {member_id} for folder {folder_name}")
                qs_target.create_folder_membership(
                    AwsAccountId=AWS_ACCOUNT_ID,
                    FolderId=folder_id,
                    MemberId=member_id,
                    MemberType=member_type,
                )
            except qs_target.exceptions.ResourceExistsException as e:
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
    start_ts = datetime.now()
    # prepare log file
    LOG_DIR.mkdir(exist_ok=True)
    logger.add(
        LOG_DIR.joinpath(
            f"migration-{source_region}-{target_region}-{start_ts.strftime('%Y-%m-%d-%H-%M-%S')}.log"
        )
    )
    QS_EXPORT_DIR.mkdir(exist_ok=True)
    QS_RETRY_DIR.mkdir(exist_ok=True)

    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    qs_source = session.client("quicksight", region_name=source_region)
    qs_target = session.client("quicksight", region_name=target_region)

    # Get assets
    logger.info("Fetching QuickSight assets...")
    source_assets = get_qs_all_assets(qs_source)

    source_data_sources = source_assets["data_sources"]
    source_data_sets = source_assets["data_sets"]
    source_analyses = source_assets["analyses"]
    source_dashboards = source_assets["dashboards"]

    source_data_source_arns = get_arns(source_data_sources, lambda x: x)
    source_data_set_arns = get_arns(source_data_sets, lambda x: x)
    source_analyses_arns = get_arns(source_analyses, lambda x: x)
    source_dashboard_arns = get_arns(source_dashboards, lambda x: x)

    all_arns = [
        *source_dashboard_arns,
        *source_analyses_arns,
        *source_data_set_arns,
        *source_data_source_arns,
    ]

    chunk_size = 100
    chunked_arns = batched(all_arns, chunk_size)
    for idx, arns in enumerate(chunked_arns):
        file_identifier = f"{idx:03}-{datetime.now().strftime('%Y-%m-%d-%H-%M%-%S')}"
        # Export assets
        try:
            asset_data = export_assets(qs_source, arns)
        except Exception as e:
            logger.error(e)
            continue
        with open(
            QS_EXPORT_DIR.joinpath(f"quicksight_asset_bundle-{file_identifier}.qs"),
            "wb",
        ) as f:
            f.write(asset_data)

        # Import assets
        import_assets(qs_target, asset_data)

    # Migrate folders and permissions
    migrate_folders_and_members(qs_source, qs_target)

    logger.info("QuickSight migration completed successfully")


if __name__ == "__main__":
    app()
