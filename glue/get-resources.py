from functools import partial
from operator import itemgetter
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import boto3
import typer
from dotenv import load_dotenv
from icecream import ic
from loguru import logger
from mypy_boto3_glue import GlueClient

load_dotenv()

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
ROOT_DIR = Path(__file__).parent.absolute()
GLUE_EXPORT_DIR = ROOT_DIR.joinpath("data")

app = typer.Typer()


def filter_dict_keys(
    dict_object: Mapping[str, Any], filter_dict_keys: Iterable
) -> Mapping:
    """Filter a dictionary to retain only specified keys."""
    return {k: v for k, v in dict_object.items() if k in filter_dict_keys}


def get_glue_databases(glue_client: GlueClient) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue databases with selected attributes."""
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
) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue tables for a given database with selected attributes."""
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


def get_glue_crawlers(glue_client: GlueClient) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue crawlers with selected attributes."""
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


def get_glue_classifiers(glue_client: GlueClient) -> Iterator[Mapping[str, Any]]:
    """Retrieve Glue classifiers with selected attributes."""
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
    classifiers = glue_client.get_classifiers().get("Classifiers", [])
    if not classifiers:
        return classifiers
    for k, v in classifiers[0].items():
        if k not in classifier_keys:
            continue
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
        except glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Crawler '{i['Name']}' already exists.")


def migrate_glue_classifier(glue_client: GlueClient, classifier_to_migrate: Iterable):
    """Migrate Glue classifiers."""
    for i in classifier_to_migrate:
        try:
            glue_client.create_classifier(**i)
        except glue_client.exceptions.AlreadyExistsException:
            logger.warning(f"Classifier '{i['Name']}' already exists.")


def glue_resource_summary(resource_name: str, resource_list: Iterable[Any]):
    """Logs the count of resources being migrated."""
    count = len(list(resource_list))
    logger.info(f"{resource_name}: {count} found.")


@app.command()
def main(region: str):
    session = boto3.Session()

    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")

    glue_source = session.client("glue", region_name=region)

    logger.info("Fetching Glue resources...")
    db_to_migrate = list(get_glue_databases(glue_source))
    db_names = list(map(itemgetter("Name"), db_to_migrate))
    db_tables_to_migrate = get_glue_db_tables(glue_source, db_names)
    classifier_to_migrate = list(get_glue_classifiers(glue_source))
    crawler_to_migrate = list(get_glue_crawlers(glue_source))

    resources = {
        "databases": db_names,
        "tables": [
            f"{db}.{_['Name']}"
            for db, tables in db_tables_to_migrate.items()
            for _ in tables
        ],
        "classifiers": [
            f"{k}-{v['Name']}" for _ in classifier_to_migrate for k, v in _.items()
        ]
        if classifier_to_migrate
        else [],
        "crawlers": [_["Name"] for _ in crawler_to_migrate]
        if crawler_to_migrate
        else [],
    }

    GLUE_EXPORT_DIR.mkdir(exist_ok=True)
    with open(GLUE_EXPORT_DIR.joinpath(f"glue_list_resources-{region}.csv"), "w") as f:
        f.write("resource_type,name")
        f.write("\n")
        for k, v in resources.items():
            if not v:
                continue
            for _ in v:
                f.write(f"{k},{_}")
                f.write("\n")


if __name__ == "__main__":
    app()
