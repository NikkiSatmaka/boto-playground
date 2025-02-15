from functools import partial
from operator import itemgetter
from typing import Any, Dict, Iterable, Iterator, Mapping

import boto3
import typer
from dotenv import load_dotenv
from loguru import logger
from mypy_boto3_glue import GlueClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

app = typer.Typer()


def filter_dict_keys(
    dict_object: Mapping[str, Any], filter_dict_keys: Iterable
) -> Mapping:
    """Filter a dictionary to retain only specified keys."""
    return {k: v for k, v in dict_object.items() if k in filter_dict_keys}


def get_glue_databases(glue_client: GlueClient) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue databases with pagination support."""
    db_keys = [
        "Name",
        "Description",
        "LocationUri",
        "Parameters",
        "CreateTableDefaultPermissions",
        "TargetDatabase",
        "FederatedDatabase",
    ]
    paginator = glue_client.get_paginator("get_databases")
    for page in paginator.paginate():
        for db in page.get("DatabaseList", []):
            yield filter_dict_keys(db, db_keys)


def get_glue_tables(
    glue_client: GlueClient, database_name: str
) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue tables for a given database with pagination support."""
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
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page.get("TableList", []):
            yield filter_dict_keys(table, table_keys)


def get_glue_crawlers(glue_client: GlueClient) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue crawlers with pagination support."""
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
    paginator = glue_client.get_paginator("get_crawlers")
    for page in paginator.paginate():
        for crawler in page.get("Crawlers", []):
            output_crawler = filter_dict_keys(crawler, crawler_keys)
            # Fix schedule type issue
            if "Schedule" in output_crawler and isinstance(
                output_crawler["Schedule"], Dict
            ):
                output_crawler["Schedule"] = output_crawler["Schedule"].get(
                    "ScheduleExpression", ""
                )
            yield output_crawler


def get_glue_classifiers(glue_client: GlueClient) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue classifiers with pagination support."""
    classifier_keys = {
        "GrokClassifier": [
            "Classification",
            "Name",
            "GrokPattern",
            "CustomPatterns",
        ],
        "XMLClassifier": [
            "Classification",
            "Name",
            "RowTag",
        ],
        "JsonClassifier": [
            "Name",
            "JsonPath",
        ],
        "CsvClassifier": [
            "Name",
            "Delimiter",
            "QuoteSymbol",
            "ContainsHeader",
            "Header",
            "DisableValueTrimming",
            "AllowSingleColumn",
            "CustomDatatypeConfigured",
            "CustomDatatypes",
            "Serde",
        ],
    }
    paginator = glue_client.get_paginator("get_classifiers")
    for page in paginator.paginate():
        for classifier in page.get("Classifiers", []):
            if not classifier:
                continue
            for k, v in classifier.items():
                if k in classifier_keys:
                    yield {k: filter_dict_keys(v, classifier_keys[k])}


def get_glue_db_tables(
    glue_client: GlueClient, db_list: Iterable
) -> Mapping[str, Iterable[Mapping[str, Any]]]:
    """Retrieve all Glue tables for a list of databases."""
    return {db: list(get_glue_tables(glue_client, db)) for db in db_list}


def migrate_glue_db(glue_client: GlueClient, db_to_migrate: Iterable):
    """Migrate Glue databases."""
    for i in db_to_migrate:
        try:
            glue_client.create_database(DatabaseInput=i)
        except glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Database '{i['Name']}' already exists.")


def migrate_glue_tables(glue_client: GlueClient, db_tables_to_migrate: Mapping):
    """Migrate Glue tables."""
    for db, tables in db_tables_to_migrate.items():
        for table in tables:
            try:
                glue_client.create_table(DatabaseName=db, TableInput=table)
                logger.info(
                    f"Table '{table['Name']}' in database '{db}' migrated successfully."
                )
            except glue_client.exceptions.AlreadyExistsException:
                logger.warning(
                    f"Table '{table['Name']}' already exists in database '{db}'."
                )


def migrate_glue_crawler(glue_client: GlueClient, crawler_to_migrate: Iterable):
    """Migrate Glue crawlers."""
    for i in crawler_to_migrate:
        try:
            glue_client.create_crawler(**i)
            logger.info(f"Crawler '{i['Name']}' migrated successfully.")
        except glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Crawler '{i['Name']}' already exists.")


def migrate_glue_classifier(glue_client: GlueClient, classifier_to_migrate: Iterable):
    """Migrate Glue classifiers."""
    for i in classifier_to_migrate:
        try:
            glue_client.create_classifier(**i)
            logger.info(
                f"Classifier '{i[list(i.keys())[0]]['Name']}' migrated successfully."
            )
        except glue_client.exceptions.AlreadyExistsException:
            logger.warning(
                f"Classifier '{i[list(i.keys())[0]]['Name']}' already exists."
            )


def glue_resource_summary(resource_name: str, resource_list: Iterable[Any]):
    """Logs the count of resources being migrated."""
    count = len(list(resource_list))
    logger.info(f"{resource_name}: {count} found.")


@app.command()
def main(source_region: str, target_region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    glue_source = session.client("glue", region_name=source_region)
    glue_target = session.client("glue", region_name=target_region)

    logger.info("Fetching Glue resources...")
    db_to_migrate = list(get_glue_databases(glue_source))
    db_names = list(map(itemgetter("Name"), db_to_migrate))
    db_tables_to_migrate = get_glue_db_tables(glue_source, db_names)
    classifier_to_migrate = list(get_glue_classifiers(glue_source))
    crawler_to_migrate = list(get_glue_crawlers(glue_source))

    glue_resource_summary("Databases", db_to_migrate)
    for db, tables in db_tables_to_migrate.items():
        logger.info(f"DB {db}: {len(list(tables))} table(s)")
    glue_resource_summary("Crawlers", crawler_to_migrate)
    glue_resource_summary("Classifiers", classifier_to_migrate)

    logger.info("Starting Glue migration process...")
    migrate_glue_db(glue_target, db_to_migrate)
    migrate_glue_tables(glue_target, db_tables_to_migrate)
    migrate_glue_classifier(glue_target, classifier_to_migrate)
    migrate_glue_crawler(glue_target, crawler_to_migrate)

    logger.info("Glue migration completed successfully.")


if __name__ == "__main__":
    app()
