import os
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

    qs_data_sources = qs_sg.list_data_sources(AwsAccountId=aws_account_id)[
        "DataSources"
    ]

    get_data_sources = itemgetter("Arn", "DataSourceId", "Name")

    datasource_names = ["smdg-master-terminal-facilities-list.csv", "wti-daily.csv"]

    ic(list(map(get_data_sources, qs_data_sources)))


if __name__ == "__main__":
    main()
