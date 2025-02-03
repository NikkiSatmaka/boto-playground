import os
from time import sleep
from operator import itemgetter

import boto3
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

session = boto3.Session(profile_name=AWS_PROFILE)


def main():
    sts = session.client("sts")
    aws_account_id = sts.get_caller_identity()["Account"]

    s3 = session.client("s3", config=config)
    qs_sg = session.client("quicksight", region_name=REGION_1)
    qs_jkt = session.client("quicksight", region_name=REGION_2)

    datasource_names = ["smdg-master-terminal-facilities-list", "wti-daily"]

    type_getter = itemgetter("Type")
    name_getter = itemgetter("Name")
    arn_getter = itemgetter("Arn")

    qs_data_sources = qs_sg.list_data_sources(AwsAccountId=aws_account_id)
    qs_data_sets = qs_sg.list_data_sets(AwsAccountId=aws_account_id)
    qs_dashboards = qs_sg.list_dashboards(AwsAccountId=aws_account_id)

    data_source_arns = list(
        map(
            lambda x: x,
            filter(
                lambda x: type_getter(x) == "ATHENA",
                qs_data_sources["DataSources"],
            ),
        )
    )
    data_set_arns = list(
        map(
            lambda x: x,
            filter(
                lambda x: x,
                qs_data_sets["DataSetSummaries"],
            ),
        )
    )
    dashboard_arns = list(
        map(
            arn_getter,
            filter(
                lambda x: name_getter(x) == "dashboard-sg",
                qs_dashboards["DashboardSummaryList"],
            ),
        )
    )

    # ic(data_source_arns)
    # ic(data_set_arns)
    ic(dashboard_arns)



if __name__ == "__main__":
    main()
