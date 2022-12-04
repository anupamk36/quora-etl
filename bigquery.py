import logging
import textwrap
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest


def get_client(project_id) -> bigquery.Client:
    client = bigquery.Client(project=project_id)
    return client


TABLE_SCHEMA = [
    bigquery.SchemaField("accountId", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("accountName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("accountCurrency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("adId", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("adName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("adSetId", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("adSetName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaignId", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("campaignName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("clicks", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("spend", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("cpc", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("ctr", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("startDate", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("endDate", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("bidAmount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("landingURL", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Generic", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("AppInstall", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("Purchase", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("GenerateLead", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("CompleteRegistration", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("AddPaymentInfo", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("AddToCart", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("AddToWishlist", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("InitiateCheckout", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("Search", "FLOAT64", mode="NULLABLE"),
]


def ensure_table(client: bigquery.Client, table_id: str) -> None:
    """
    This function is used to ensure if table is present
    at destination otherwise creates a new table.
    """
    table = bigquery.Table(table_id, schema=TABLE_SCHEMA)
    client.create_table(table, exists_ok=True)


def upload_new_data(client: bigquery.Client, table_id: str, file_name: str) -> None:
    ensure_table(client, table_id)
    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    with open(file_name, "rb") as file:
        job = client.load_table_from_file(file, table_id, job_config=job_config)

    try:
        job.result()
        logging.info(f"UPLOAD NEW TABLE {table_id} DONE")
    except BadRequest:
        err = job.errors or []
        for e in err:
            logging.error("ERROR: {}".format(e["message"]))
        raise


def save_to_bigquery(project_id: str, results_file_name: str = "data.json") -> None:
    """
    This function writes the latest records
    to a json locally and then to destination
    BigQuery table.
    """
    client = get_client(project_id=project_id)
    dataset = "quora"

    tmp_table_id = f"{project_id}.{dataset}.quora_ads_tmp"
    upload_new_data(client, tmp_table_id, results_file_name)

    table_id = f"{project_id}.{dataset}.quora_ads"
    ensure_table(client, table_id)

    update_cols_list = [
        "accountName",
        "adName",
        "adSetName",
        "campaignName",
        "accountCurrency",
        "clicks",
        "impressions",
        "spend",
        "cpc",
        "ctr",
        "endDate",
        "bidAmount",
        "landingURL",
        "status",
        "Generic",
        "AppInstall",
        "Purchase",
        "GenerateLead",
        "CompleteRegistration",
        "AddPaymentInfo",
        "AddToCart",
        "AddToWishlist",
        "InitiateCheckout",
        "Search",
    ]
    insert_cols_list = update_cols_list + [
        "accountId",
        "adId",
        "adSetId",
        "campaignId",
        "startDate",
    ]

    update_cols = ",\n".join(f"{c} = tmp.{c}" for c in update_cols_list)
    insert_cols = ",\n".join(insert_cols_list)
    value_cols = ",\n".join(f"tmp.{c}" for c in insert_cols_list)

    def indent(string: str) -> str:
        return textwrap.indent(string, " " * 8)

    query = f"""
    merge {table_id} as old
    using {tmp_table_id} as tmp
      on old.adId = tmp.adId
      and old.startDate = tmp.startDate
      and old.accountId = tmp.accountId
      and old.adSetId = tmp.adSetId
      and old.campaignId = tmp.campaignId
    when matched then
      update set
        {indent(update_cols)}
    when not matched then
      insert (
        {indent(insert_cols)}
      )
      values (
        {indent(value_cols)}
      )
    """
    logging.info(f"MERGE QUERY:\n{textwrap.dedent(query)}")

    job = client.query(query)
    try:
        job.result()
        logging.info(f"MERGE TABLE {table_id} FROM FILE DONE!")
    except BadRequest:
        err = job.errors or []
        for error in err:
            logging.error("ERROR: %s", error["message"])

    client.delete_table(tmp_table_id)
