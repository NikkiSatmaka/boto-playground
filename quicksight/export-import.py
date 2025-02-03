import os
from functools import partial
from operator import itemgetter
from pathlib import Path
from time import sleep

import boto3
import httpx
from botocore.config import Config
from dotenv import load_dotenv
from icecream import ic

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
AWS_PROFILE = os.environ["AWS_PROFILE"]
REGION_1 = os.environ["REGION_1"]
REGION_2 = os.environ["REGION_2"]

config = Config(
    region_name=AWS_REGION,
)

ROOT_DIR = Path(__file__).parent

session = boto3.Session(profile_name=AWS_PROFILE)


def main():
    sts = session.client("sts")
    aws_account_id = sts.get_caller_identity()["Account"]

    default_kwargs = {"AwsAccountId": aws_account_id}

    s3 = session.client("s3", config=config)
    qs_sg = session.client("quicksight", region_name=REGION_1)
    qs_jkt = session.client("quicksight", region_name=REGION_2)

    datasource_names = ["data-test12345"]
    dataset_names = ["netflix_data"]
    analysis_names = ["netflix_data analysis-test12345"]
    dashboard_names = ["netflix_data_dashboard-test12345"]

    asset_export_job_name = "test-1"
    asset_import_job_name = "test-1"

    type_getter = itemgetter("Type")
    name_getter = itemgetter("Name")
    arn_getter = itemgetter("Arn")

    qs_data_sources = qs_jkt.list_data_sources(**default_kwargs)
    qs_data_sets = qs_jkt.list_data_sets(**default_kwargs)
    qs_analysis = qs_jkt.list_analyses(**default_kwargs)
    qs_dashboards = qs_jkt.list_dashboards(**default_kwargs)

    def get_keys(data_sources, func_getter, criteria):
        return list(map(func_getter, filter(criteria, data_sources)))

    def get_arns(data_sources, criteria):
        return get_keys(data_sources, arn_getter, criteria)

    def data_source_criteria(data_source):
        return (
            type_getter(data_source) == "ATHENA"
            and name_getter(data_source) in datasource_names
        )

    def data_set_criteria(data_set):
        return (
            itemgetter("ImportMode")(data_set) == "SPICE"
            and name_getter(data_set) in dataset_names
        )

    def analysis_criteria(analysis):
        return (
            # itemgetter("ImportMode")(analysis) == "SPICE"
            name_getter(analysis) in analysis_names
        )

    def dashboard_criteria(dashboard):
        return name_getter(dashboard) in dashboard_names

    data_source_arns = get_arns(
        qs_data_sources["DataSources"],
        data_source_criteria,
    )

    data_set_arns = get_arns(
        qs_data_sets["DataSetSummaries"],
        data_set_criteria,
    )

    analysis_arns = get_arns(
        qs_analysis["AnalysisSummaryList"],
        analysis_criteria,
    )

    dashboard_arns = get_arns(
        qs_dashboards["DashboardSummaryList"],
        dashboard_criteria,
    )

    data_source_ids = get_keys(
        qs_data_sources["DataSources"],
        itemgetter("DataSourceId"),
        data_source_criteria,
    )

    data_set_ids = get_keys(
        qs_data_sets["DataSetSummaries"], itemgetter("DataSetId"), data_set_criteria
    )

    analysis_ids = get_keys(
        qs_analysis["AnalysisSummaryList"],
        itemgetter("AnalysisId"),
        analysis_criteria,
    )

    dashboard_ids = get_keys(
        qs_dashboards["DashboardSummaryList"],
        itemgetter("DashboardId"),
        dashboard_criteria,
    )

    ic(data_source_arns)
    ic(data_set_arns)
    ic(analysis_arns)
    ic(dashboard_arns)

    ic(data_source_ids)
    ic(data_set_ids)
    ic(analysis_ids)
    ic(dashboard_ids)

    # for i in get_keys(
    #     qs_data_sources["DataSources"],
    #     itemgetter("DataSourceId"),
    #     lambda x: x,
    # ):
    #     qs_jkt.delete_data_source(**default_kwargs, DataSourceId=i)
    # for i in get_keys(
    #     qs_data_sets["DataSetSummaries"], itemgetter("DataSetId"), lambda x: x
    # ):
    #     qs_jkt.delete_data_set(**default_kwargs, DataSetId=i)
    # for i in get_keys(
    #     qs_analysis["AnalysisSummaryList"],
    #     itemgetter("AnalysisId"),
    #     lambda x: x,
    # ):
    #     qs_jkt.delete_analysis(
    #         AwsAccountId=aws_account_id, AnalysisId=i, ForceDeleteWithoutRecovery=True
    #     )
    # for i in get_keys(
    #     qs_dashboards["DashboardSummaryList"],
    #     itemgetter("DashboardId"),
    #     lambda x: x,
    # ):
    #     qs_jkt.delete_dashboard(AwsAccountId=aws_account_id, DashboardId=i)

    def export_asset_bundle(asset_export_job_name, *resource_arns, **kwargs):
        r = qs_sg.start_asset_bundle_export_job(
            AwsAccountId=aws_account_id,
            AssetBundleExportJobId=asset_export_job_name,
            ResourceArns=[*resource_arns],
            IncludeAllDependencies=True,
            ExportFormat="QUICKSIGHT_JSON",
            **kwargs,
        )
        return r

    export_response = export_asset_bundle(asset_export_job_name, *dashboard_arns)

    while True:
        probe_export_response = qs_sg.describe_asset_bundle_export_job(
            AwsAccountId=aws_account_id,
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

    import_response = qs_jkt.start_asset_bundle_import_job(
        AwsAccountId=aws_account_id,
        AssetBundleImportJobId=asset_import_job_name,
        AssetBundleImportSource={"Body": r.content},
    )
    ic(import_response)

    while True:
        probe_import_response = qs_jkt.describe_asset_bundle_import_job(
            AwsAccountId=aws_account_id,
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
    main()
