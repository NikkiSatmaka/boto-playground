import os
from operator import itemgetter
from pathlib import Path
from time import sleep

import boto3
import httpx
import typer
from dotenv import load_dotenv
from icecream import ic
from loguru import logger
from mypy_boto3_quicksight import QuickSightClient

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
AWS_PROFILE = os.environ["AWS_PROFILE"]
REGION_1 = os.environ["REGION_1"]
REGION_2 = os.environ["REGION_2"]
AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

ROOT_DIR = Path(__file__).parent

app = typer.Typer()

datasource_name = "data-test12345"
dataset_name = "netflix_data"
analysis_name = "netflix_data analysis-test12345"
dashboard_name = "netflix_data_dashboard-test12345"
asset_export_job_name = "test-1"
asset_import_job_name = "test-1"

type_getter = itemgetter("Type")
name_getter = itemgetter("Name")
arn_getter = itemgetter("Arn")


def get_all(qs_object: QuickSightClient) -> dict:
    data_sources = qs_object.list_data_sources(AwsAccountId=AWS_ACCOUNT_ID)
    data_sets = qs_object.list_data_sets(AwsAccountId=AWS_ACCOUNT_ID)
    analysis = qs_object.list_analyses(AwsAccountId=AWS_ACCOUNT_ID)
    dashboards = qs_object.list_dashboards(AwsAccountId=AWS_ACCOUNT_ID)

    return {
        "data_source": data_sources["DataSources"],
        "data_sets": data_sets["DataSetSummaries"],
        "analysis": analysis["AnalysisSummaryList"],
        "dashboards": dashboards["DashboardSummaryList"],
    }


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

    source_data_sources = get_all(qs_source)["data_source"]
    source_data_sets = get_all(qs_source)["data_sets"]
    source_analysis = get_all(qs_source)["analysis"]
    source_dashboards = get_all(qs_source)["dashboards"]

    target_data_sources = get_all(qs_target)["data_source"]
    target_data_sets = get_all(qs_target)["data_sets"]
    target_analysis = get_all(qs_target)["analysis"]
    target_dashboards = get_all(qs_target)["dashboards"]

    source_data_source_arns = get_arns(source_data_sources, data_source_criteria)
    source_data_set_arns = get_arns(source_data_sets, data_set_criteria)
    source_analysis_arns = get_arns(source_analysis, analysis_criteria)
    source_dashboard_arns = get_arns(source_dashboards, dashboard_criteria)

    source_data_source_ids = get_keys(
        source_data_sources, itemgetter("DataSourceId"), data_source_criteria
    )
    source_data_set_ids = get_keys(
        source_data_sets, itemgetter("DataSetId"), data_set_criteria
    )
    source_analysis_ids = get_keys(
        source_analysis, itemgetter("AnalysisId"), analysis_criteria
    )
    source_dashboard_ids = get_keys(
        source_dashboards, itemgetter("DashboardId"), dashboard_criteria
    )

    target_data_source_arns = get_arns(target_data_sources, data_source_criteria)
    target_data_set_arns = get_arns(target_data_sets, data_set_criteria)
    target_analysis_arns = get_arns(target_analysis, analysis_criteria)
    target_dashboard_arns = get_arns(target_dashboards, dashboard_criteria)

    target_data_source_ids = get_keys(
        target_data_sources, itemgetter("DataSourceId"), data_source_criteria
    )
    target_data_set_ids = get_keys(
        target_data_sets, itemgetter("DataSetId"), data_set_criteria
    )
    target_analysis_ids = get_keys(
        target_analysis, itemgetter("AnalysisId"), analysis_criteria
    )
    target_dashboard_ids = get_keys(
        target_dashboards, itemgetter("DashboardId"), dashboard_criteria
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

    export_response = qs_source.start_asset_bundle_export_job(
        AwsAccountId=AWS_ACCOUNT_ID,
        AssetBundleExportJobId=asset_export_job_name,
        ResourceArns=[*source_dashboard_arns],
        IncludeAllDependencies=True,
        ExportFormat="QUICKSIGHT_JSON",
        IncludePermissions=True,
        IncludeFolderMemberships=True,
        IncludeTags=True,
    )

    while True:
        probe_export_response = qs_source.describe_asset_bundle_export_job(
            AwsAccountId=AWS_ACCOUNT_ID,
            AssetBundleExportJobId=export_response["AssetBundleExportJobId"],
        )
        if probe_export_response["JobStatus"] in [
            "SUCCESSFUL",
            "FAILED",
        ]:
            break
        sleep(2)

    r = httpx.get(probe_export_response["DownloadUrl"])
    with open(
        ROOT_DIR.joinpath(f"data/asset-bundle/assetbundle-{asset_export_job_name}.qs"),
        "wb",
    ) as f:
        f.write(r.content)

    ic(probe_export_response)
    ic(export_response)

    import_response = qs_target.start_asset_bundle_import_job(
        AwsAccountId=AWS_ACCOUNT_ID,
        AssetBundleImportJobId=asset_import_job_name,
        AssetBundleImportSource={"Body": r.content},
    )

    while True:
        probe_import_response = qs_target.describe_asset_bundle_import_job(
            AwsAccountId=AWS_ACCOUNT_ID,
            AssetBundleImportJobId=import_response["AssetBundleImportJobId"],
        )
        if probe_import_response["JobStatus"] in [
            "SUCCESSFUL",
            "FAILED",
            "FAILED_ROLLBACK_COMPLETED",
            "FAILED_ROLLBACK_ERROR",
        ]:
            break
        sleep(2)

    ic(probe_import_response)
    ic(import_response)


if __name__ == "__main__":
    app()
