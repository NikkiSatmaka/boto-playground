import os
from operator import itemgetter

import boto3
import typer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
AWS_PROFILE = os.environ["AWS_PROFILE"]
REGION = os.environ["AWS_REGION"]

app = typer.Typer()


@app.command()
def main(region: str):
    session = boto3.Session()
    sts = session.client("sts")
    aws_account_id = sts.get_caller_identity()["Account"]

    logger.info(f"AWS Account ID: {aws_account_id}")

    qs = session.client("quicksight", region_name=region)

    qs_data_sources = qs.list_data_sources(AwsAccountId=aws_account_id)
    qs_data_sets = qs.list_data_sets(AwsAccountId=aws_account_id)
    qs_analysis = qs.list_analyses(AwsAccountId=aws_account_id)
    qs_dashboards = qs.list_dashboards(AwsAccountId=aws_account_id)

    def get_keys(data_sources, func_getter, criteria):
        return list(map(func_getter, filter(criteria, data_sources)))

    logger.debug("Deleting Data Sources")
    for i in get_keys(
        qs_data_sources["DataSources"], itemgetter("DataSourceId"), lambda x: x
    ):
        logger.debug(f"Deleting Data Source: {i}")
        qs.delete_data_source(AwsAccountId=aws_account_id, DataSourceId=i)

    logger.debug("Deleting Data Sets")
    for i in get_keys(
        qs_data_sets["DataSetSummaries"], itemgetter("DataSetId"), lambda x: x
    ):
        logger.debug(f"Deleting Data Set: {i}")
        qs.delete_data_set(AwsAccountId=aws_account_id, DataSetId=i)

    logger.debug("Deleting Analysis")
    for i in get_keys(
        qs_analysis["AnalysisSummaryList"], itemgetter("AnalysisId"), lambda x: x
    ):
        logger.debug(f"Deleting Analysis: {i}")
        qs.delete_analysis(
            AwsAccountId=aws_account_id, AnalysisId=i, ForceDeleteWithoutRecovery=True
        )

    logger.debug("Deleting Dashboards")
    for i in get_keys(
        qs_dashboards["DashboardSummaryList"], itemgetter("DashboardId"), lambda x: x
    ):
        logger.debug(f"Deleting Dashboard: {i}")
        qs.delete_dashboard(AwsAccountId=aws_account_id, DashboardId=i)


if __name__ == "__main__":
    app()
