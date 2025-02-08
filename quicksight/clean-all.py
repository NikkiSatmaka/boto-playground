from operator import itemgetter

import boto3
import typer
from dotenv import load_dotenv
from loguru import logger
from mypy_boto3_quicksight import QuickSightClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

app = typer.Typer()


def delete_quicksight_resources(qs_client: QuickSightClient):
    qs_dashboards = qs_client.list_dashboards(AwsAccountId=AWS_ACCOUNT_ID)
    qs_analysis = qs_client.list_analyses(AwsAccountId=AWS_ACCOUNT_ID)
    qs_data_sets = qs_client.list_data_sets(AwsAccountId=AWS_ACCOUNT_ID)
    qs_data_sources = qs_client.list_data_sources(AwsAccountId=AWS_ACCOUNT_ID)

    def get_keys(data_sources, func_getter, criteria):
        return list(map(func_getter, filter(criteria, data_sources)))

    logger.debug("Deleting Dashboards")
    for i in get_keys(
        qs_dashboards["DashboardSummaryList"], itemgetter("DashboardId"), lambda x: x
    ):
        logger.debug(f"Deleting Dashboard: {i}")
        qs_client.delete_dashboard(AwsAccountId=AWS_ACCOUNT_ID, DashboardId=i)

    logger.debug("Deleting Analysis")
    for i in get_keys(
        qs_analysis["AnalysisSummaryList"], itemgetter("AnalysisId"), lambda x: x
    ):
        logger.debug(f"Deleting Analysis: {i}")
        qs_client.delete_analysis(
            AwsAccountId=AWS_ACCOUNT_ID, AnalysisId=i, ForceDeleteWithoutRecovery=True
        )

    logger.debug("Deleting Data Sets")
    for i in get_keys(
        qs_data_sets["DataSetSummaries"], itemgetter("DataSetId"), lambda x: x
    ):
        logger.debug(f"Deleting Data Set: {i}")
        qs_client.delete_data_set(AwsAccountId=AWS_ACCOUNT_ID, DataSetId=i)

    logger.debug("Deleting Data Sources")
    for i in get_keys(
        qs_data_sources["DataSources"], itemgetter("DataSourceId"), lambda x: x
    ):
        logger.debug(f"Deleting Data Source: {i}")
        qs_client.delete_data_source(AwsAccountId=AWS_ACCOUNT_ID, DataSourceId=i)


@app.command()
def main(region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    qs = session.client("quicksight", region_name=region)

    delete_quicksight_resources(qs)


if __name__ == "__main__":
    app()
