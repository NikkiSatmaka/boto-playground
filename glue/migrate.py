from functools import partial
from operator import itemgetter
from pathlib import Path

import boto3
import typer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

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


def filter_dict_keys(dict_object, filter_dict_keys):
    return {k: v for k, v in dict_object.items() if k in filter_dict_keys}


def get_db_generator(glue_client):
    db_keys = [
        "Name",
        "Description",
        "LocationUri",
        "Parameters",
        "CreateTableDefaultPermissions",
        "TargetDatabase",
        "FederatedDatabase",
    ]
    return map(
        partial(filter_dict_keys, filter_dict_keys=db_keys),
        glue_client.get_databases()["DatabaseList"],
    )


def get_table_generator(glue_client, database_name):
    table_keys = [
        "Name",
        "Description",
        "Owner",
        "LastAccessTime",
        "LastAnalyzedTime",
        "Retention",
        "StorageDescriptor",
        "PartitionKeys",
        "ViewOriginalText",
        "ViewExpandedText",
        "TableType",
        "Parameters",
        "TargetTable",
        "ViewDefinition",
    ]
    return map(
        partial(filter_dict_keys, filter_dict_keys=table_keys),
        glue_client.get_tables(DatabaseName=database_name)["TableList"],
    )


def get_crawler_generator(glue_client):
    crawler_keys = [
        "Name",
        "Role",
        "DatabaseName",
        "Description",
        "Targets",
        "Schedule",
        "Classifiers",
        "TablePrefix",
        "SchemaChangePolicy",
        "RecrawlPolicy",
        "LineageConfiguration",
        "LakeFormationConfiguration",
        "Configuration",
        "CrawlerSecurityConfiguration",
    ]
    return map(
        partial(filter_dict_keys, filter_dict_keys=crawler_keys),
        glue_client.get_crawlers()["Crawlers"],
    )


def get_classifier_generator(glue_client):
    classifier_keys = [
        "GrokClassifier",
        "XMLClassifier",
        "JsonClassifier",
        "CsvClassifier",
    ]
    return map(
        partial(filter_dict_keys, filter_dict_keys=classifier_keys),
        glue_client.get_classifiers()["Classifiers"],
    )


def get_db_table_generator(glue_client, db_list):
    return {db: list(get_table_generator(glue_client, db)) for db in db_list}


def migrate_glue_db(glue_client, db_to_migrate):
    for i in db_to_migrate:
        try:
            glue_client.create_database(DatabaseInput=i)
        except glue_client.exceptions.AlreadyExistsException as e:
            logger.error(f"{i['Name']}: {e}")


def migrate_glue_tables(glue_client, db_tables_to_migrate):
    for db, tables in db_tables_to_migrate.items():
        for table in tables:
            try:
                glue_client.create_table(DatabaseName=db, TableInput=table)
            except glue_client.exceptions.AlreadyExistsException as e:
                logger.error(f"{table['Name']}: {e}")


def migrate_glue_crawler(glue_client, crawler_to_migrate):
    for i in crawler_to_migrate:
        try:
            glue_client.create_crawler(**i)
        except glue_client.exceptions.AlreadyExistsException as e:
            logger.error(f"{i['Name']}: {e}")


def migrate_glue_classifier(glue_client, classifier_to_migrate):
    for i in classifier_to_migrate:
        try:
            glue_client.create_classifier(**i)
        except glue_client.exceptions.AlreadyExistsException as e:
            logger.error(f"{e}")


@app.command()
def main(source_region: str, target_region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    glue_source = session.client("glue", region_name=source_region)
    glue_target = session.client("glue", region_name=target_region)

    db_to_migrate = list(get_db_generator(glue_source))
    db_tables_to_migrate = get_db_table_generator(
        glue_source, map(itemgetter("Name"), db_to_migrate)
    )
    crawler_to_migrate = list(get_crawler_generator(glue_source))
    classifier_to_migrate = list(get_classifier_generator(glue_source))

    migrate_glue_db(glue_target, db_to_migrate)
    migrate_glue_tables(glue_target, db_tables_to_migrate)
    migrate_glue_crawler(glue_target, crawler_to_migrate)
    migrate_glue_classifier(glue_target, classifier_to_migrate)


if __name__ == "__main__":
    app()
