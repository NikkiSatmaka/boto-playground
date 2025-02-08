from functools import partial
from operator import itemgetter
from typing import Any, Dict, Iterable, Iterator, Mapping

import boto3
import typer
from dotenv import load_dotenv
from loguru import logger
from mypy_boto3_glue.client import GlueClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

app = typer.Typer()


def filter_dict_keys(
    dict_object: Mapping[str, Any], filter_dict_keys: Iterable
) -> Dict:
    return {k: v for k, v in dict_object.items() if k in filter_dict_keys}


def get_glue_databases(glue_client: GlueClient) -> Iterator[Dict[str, Any]]:
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
        glue_client.get_databases().get("DatabaseList", []),
    )


def get_glue_tables(
    glue_client: GlueClient, database_name: str
) -> Iterator[Dict[str, Any]]:
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
        glue_client.get_tables(DatabaseName=database_name).get("TableList", []),
    )


def get_glue_crawlers(glue_client: GlueClient) -> Iterator[Dict[str, Any]]:
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
        glue_client.get_crawlers().get("Crawlers", []),
    )


def get_glue_classifiers(glue_client: GlueClient) -> Iterator[Dict[str, Any]]:
    classifier_keys = [
        "GrokClassifier",
        "XMLClassifier",
        "JsonClassifier",
        "CsvClassifier",
    ]
    return map(
        partial(filter_dict_keys, filter_dict_keys=classifier_keys),
        glue_client.get_classifiers().get("Classifiers", []),
    )


def get_glue_db_tables(
    glue_client: GlueClient, db_list: Iterable
) -> Dict[str, Iterable[Dict[str, Any]]]:
    return {db: list(get_glue_tables(glue_client, db)) for db in db_list}


def migrate_glue_db(glue_client: GlueClient, db_to_migrate: Iterable):
    for i in db_to_migrate:
        try:
            glue_client.create_database(DatabaseInput=i)
        except glue_client.exceptions.AlreadyExistsException as e:
            logger.error(f"{i['Name']}: {e}")


def migrate_glue_tables(glue_client: GlueClient, db_tables_to_migrate: Dict):
    for db, tables in db_tables_to_migrate.items():
        for table in tables:
            try:
                glue_client.create_table(DatabaseName=db, TableInput=table)
            except glue_client.exceptions.AlreadyExistsException as e:
                logger.error(f"{table['Name']}: {e}")


def migrate_glue_crawler(glue_client: GlueClient, crawler_to_migrate: Iterable):
    for i in crawler_to_migrate:
        try:
            glue_client.create_crawler(**i)
        except glue_client.exceptions.AlreadyExistsException as e:
            logger.error(f"{i['Name']}: {e}")


def migrate_glue_classifier(glue_client: GlueClient, classifier_to_migrate: Iterable):
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

    db_to_migrate = list(get_glue_databases(glue_source))
    db_tables_to_migrate = get_glue_db_tables(
        glue_source, map(itemgetter("Name"), db_to_migrate)
    )
    crawler_to_migrate = list(get_glue_crawlers(glue_source))
    classifier_to_migrate = list(get_glue_classifiers(glue_source))

    migrate_glue_db(glue_target, db_to_migrate)
    migrate_glue_tables(glue_target, db_tables_to_migrate)
    migrate_glue_crawler(glue_target, crawler_to_migrate)
    migrate_glue_classifier(glue_target, classifier_to_migrate)


if __name__ == "__main__":
    app()
