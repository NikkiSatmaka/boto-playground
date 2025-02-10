from operator import itemgetter
from typing import Iterable

import boto3
import typer
from dotenv import load_dotenv
from loguru import logger
from mypy_boto3_quicksight import QuickSightClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

app = typer.Typer()


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


def delete_quicksight_resources(qs_client: QuickSightClient):
    qs_dashboards = qs_client.list_dashboards(AwsAccountId=AWS_ACCOUNT_ID)[
        "DashboardSummaryList"
    ]
    qs_analysis = qs_client.list_analyses(AwsAccountId=AWS_ACCOUNT_ID)[
        "AnalysisSummaryList"
    ]
    qs_data_sets = qs_client.list_data_sets(AwsAccountId=AWS_ACCOUNT_ID)[
        "DataSetSummaries"
    ]
    qs_data_sources = qs_client.list_data_sources(AwsAccountId=AWS_ACCOUNT_ID)[
        "DataSources"
    ]
    qs_folders = get_qs_folders_sorted(qs_client, reverse=True)

    def get_keys(data_sources, func_getter, criteria):
        return list(map(func_getter, filter(criteria, data_sources)))

    logger.debug("Deleting Dashboards")
    for i in get_keys(qs_dashboards, itemgetter("DashboardId"), lambda x: x):
        logger.debug(f"Deleting Dashboard: {i}")
        qs_client.delete_dashboard(AwsAccountId=AWS_ACCOUNT_ID, DashboardId=i)

    logger.debug("Deleting Analysis")
    for i in get_keys(qs_analysis, itemgetter("AnalysisId"), lambda x: x):
        logger.debug(f"Deleting Analysis: {i}")
        qs_client.delete_analysis(
            AwsAccountId=AWS_ACCOUNT_ID, AnalysisId=i, ForceDeleteWithoutRecovery=True
        )

    logger.debug("Deleting Data Sets")
    for i in get_keys(qs_data_sets, itemgetter("DataSetId"), lambda x: x):
        logger.debug(f"Deleting Data Set: {i}")
        qs_client.delete_data_set(AwsAccountId=AWS_ACCOUNT_ID, DataSetId=i)

    logger.debug("Deleting Data Sources")
    for i in get_keys(qs_data_sources, itemgetter("DataSourceId"), lambda x: x):
        logger.debug(f"Deleting Data Source: {i}")
        qs_client.delete_data_source(AwsAccountId=AWS_ACCOUNT_ID, DataSourceId=i)

    logger.debug("Deleting Folders")
    for i in get_keys(qs_folders, itemgetter("FolderId"), lambda x: x):
        logger.debug("Deleting Folder Members")
        folder_members = qs_client.list_folder_members(
            AwsAccountId=AWS_ACCOUNT_ID, FolderId=i
        )["FolderMemberList"]
        for j in folder_members:
            logger.debug(f"Deleting Member: {j} from folder {i}")
            qs_client.delete_folder_membership(
                AwsAccountId=AWS_ACCOUNT_ID,
                FolderId=i,
                MemberId=j.get("MemberId", ""),
                MemberType=j.get("MemberArn", []).split(":")[-1].split("/")[0].upper(),  # type: ignore
            )
        logger.debug(f"Deleting Folder: {i}")
        qs_client.delete_folder(AwsAccountId=AWS_ACCOUNT_ID, FolderId=i)


@app.command()
def main(region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    qs = session.client("quicksight", region_name=region)

    delete_quicksight_resources(qs)


if __name__ == "__main__":
    app()
