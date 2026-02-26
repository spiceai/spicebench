#!/usr/bin/env python3
"""
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

"""Cleanup schemas created by spicebench benchmarks.

Subcommands:
  cleanup-uc-schema       Drop all tables in a Unity Catalog schema via SQL Statements API
  cleanup-lakebase-schema Drop a Lakebase PostgreSQL schema (CASCADE) via direct PG connection
"""

import argparse
import logging
import os
import sys
import time
import uuid
from typing import Optional

import requests

LOG_FORMAT = "[cleanup] %(levelname)s: %(message)s"
logger = logging.getLogger("cleanup_schema")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def quoted_identifier(name: str) -> str:
    """Backtick-quote a Databricks SQL identifier, escaping internal backticks."""
    return "`" + name.replace("`", "``") + "`"


def validate_endpoint(endpoint: str) -> None:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        logger.error("Endpoint should be hostname only (no scheme): %s", endpoint)
        sys.exit(1)
    if "/" in endpoint:
        logger.error("Endpoint should not include path segments: %s", endpoint)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Databricks SQL Statements API
# ---------------------------------------------------------------------------

def poll_statement_status(
    endpoint: str, token: str, statement_id: str, timeout: int = 180
) -> None:
    url = f"https://{endpoint}/api/2.0/sql/statements/{statement_id}"
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Statement status check failed ({resp.status_code}): {resp.text}"
            )

        body = resp.json()
        state = body["status"]["state"]

        if state == "SUCCEEDED":
            logger.info("Statement %s succeeded", statement_id)
            return
        if state == "FAILED":
            error_msg = body["status"].get("error", {}).get("message", "unknown error")
            raise RuntimeError(f"Statement {statement_id} failed: {error_msg}")
        if state == "CANCELED":
            raise RuntimeError(f"Statement {statement_id} canceled")

        logger.debug("Statement %s state: %s, polling...", statement_id, state)
        time.sleep(0.75)

    raise RuntimeError(f"Timed out waiting for statement {statement_id}")


def execute_databricks_sql(
    endpoint: str,
    token: str,
    warehouse_id: str,
    catalog: str,
    schema: str,
    statement: str,
    dry_run: bool = False,
) -> None:
    if dry_run:
        logger.info("[DRY RUN] Would execute SQL: %s", statement)
        return

    url = f"https://{endpoint}/api/2.0/sql/statements/"
    payload = {
        "warehouse_id": warehouse_id,
        "catalog": catalog,
        "schema": schema,
        "statement": statement,
        "wait_timeout": "20s",
    }
    headers = {"Authorization": f"Bearer {token}"}

    logger.info("Executing SQL: %s", statement)
    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Databricks SQL execute failed ({resp.status_code}): {resp.text}"
        )

    body = resp.json()
    state = body["status"]["state"]

    if state == "SUCCEEDED":
        logger.info("Statement succeeded")
        return
    if state == "FAILED":
        error_msg = body["status"].get("error", {}).get("message", "unknown error")
        raise RuntimeError(f"Databricks statement failed: {error_msg}")
    if state == "CANCELED":
        raise RuntimeError("Databricks statement canceled")
    if state in ("PENDING", "RUNNING"):
        poll_statement_status(endpoint, token, body["statement_id"])


# ---------------------------------------------------------------------------
# Lakebase PG token generation
# ---------------------------------------------------------------------------

def generate_lakebase_pg_token(
    endpoint: str,
    token: str,
    database_instance: Optional[str],
    project: Optional[str],
    branch: str,
) -> str:
    headers = {"Authorization": f"Bearer {token}"}
    request_id = str(uuid.uuid4())

    if database_instance:
        url = f"https://{endpoint}/api/2.0/database/credentials"
        payload = {
            "request_id": request_id,
            "instance_names": [database_instance],
        }
    else:
        endpoint_path = f"projects/{project}/branches/{branch}/endpoints/default"
        url = f"https://{endpoint}/api/2.0/postgres/generate-database-credential"
        payload = {
            "request_id": request_id,
            "endpoint": endpoint_path,
        }

    logger.info("Generating Lakebase PG token...")
    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    if not resp.ok:
        raise RuntimeError(
            f"Failed to generate Lakebase credential ({resp.status_code}): {resp.text}"
        )

    cred = resp.json()
    expiration = cred.get("expiration_time") or cred.get("expire_time") or "unknown"
    logger.info("Lakebase PG token generated, expires: %s", expiration)
    return cred["token"]


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def list_uc_tables(endpoint: str, token: str, catalog: str, schema: str) -> list:
    """List all table names in a Unity Catalog schema."""
    url = f"https://{endpoint}/api/2.1/unity-catalog/tables"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"catalog_name": catalog, "schema_name": schema}

    tables = []
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"UC tables list failed ({resp.status_code}): {resp.text}"
            )
        body = resp.json()
        for t in body.get("tables", []):
            tables.append(t["name"])
        page_token = body.get("next_page_token")
        if not page_token:
            break
        params["page_token"] = page_token

    return tables


def cleanup_uc_schema(args: argparse.Namespace) -> None:
    validate_endpoint(args.endpoint)

    if args.dry_run:
        logger.info(
            "[DRY RUN] Would list tables via GET /api/2.1/unity-catalog/tables"
            " (catalog=%s, schema=%s)", args.catalog, args.schema,
        )
        logger.info(
            "[DRY RUN] Would execute DROP TABLE IF EXISTS %s.%s.<table> for each table",
            quoted_identifier(args.catalog), quoted_identifier(args.schema),
        )
        return

    logger.info("Listing tables in %s.%s ...", args.catalog, args.schema)
    tables = list_uc_tables(args.endpoint, args.token, args.catalog, args.schema)

    if not tables:
        logger.info("No tables found in %s.%s", args.catalog, args.schema)
        return

    logger.info("Found %d table(s): %s", len(tables), ", ".join(tables))

    for table_name in tables:
        cat = quoted_identifier(args.catalog)
        sch = quoted_identifier(args.schema)
        tbl = quoted_identifier(table_name)
        statement = f"DROP TABLE IF EXISTS {cat}.{sch}.{tbl}"

        execute_databricks_sql(
            args.endpoint,
            args.token,
            args.warehouse_id,
            args.catalog,
            args.schema,
            statement,
        )

    logger.info("Dropped %d table(s) from %s.%s", len(tables), args.catalog, args.schema)


def cleanup_lakebase_schema(args: argparse.Namespace) -> None:
    validate_endpoint(args.endpoint)

    safe_schema = args.pg_schema.replace('"', '""')
    statement = f'DROP SCHEMA IF EXISTS "{safe_schema}" CASCADE'

    if args.dry_run:
        logger.info("[DRY RUN] Would generate PG token from %s", args.endpoint)
        logger.info(
            "[DRY RUN] Would connect to postgresql://%s:***@%s/%s?sslmode=require",
            args.pg_user, args.pg_host, args.pg_db_name,
        )
        logger.info("[DRY RUN] Would execute: %s", statement)
        return

    pg_token = generate_lakebase_pg_token(
        args.endpoint,
        args.token,
        args.database_instance,
        args.project,
        args.branch,
    )

    import psycopg2  # noqa: lazy import — only needed for lakebase subcommand

    conn_string = (
        f"host={args.pg_host} dbname={args.pg_db_name} user={args.pg_user} "
        f"password={pg_token} sslmode=require"
    )

    logger.info("Connecting to Lakebase PG at %s/%s", args.pg_host, args.pg_db_name)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            logger.info("Executing: %s", statement)
            cur.execute(statement)
            logger.info('Lakebase PG schema "%s" dropped', args.pg_schema)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cleanup schemas created by spicebench benchmarks.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- cleanup-uc-schema ------------------------------------------------
    uc = subparsers.add_parser("cleanup-uc-schema", help="Drop all tables in a UC schema")
    uc.add_argument("--endpoint", default=os.environ.get("DATABRICKS_ENDPOINT", ""),
                    help="Databricks hostname (env: DATABRICKS_ENDPOINT)")
    uc.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""),
                    help="Databricks PAT (env: DATABRICKS_TOKEN)")
    uc.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", ""),
                    help="SQL warehouse ID (env: DATABRICKS_SQL_WAREHOUSE_ID)")
    uc.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG", ""),
                    help="UC catalog name (env: DATABRICKS_CATALOG)")
    uc.add_argument("--schema", default=os.environ.get("DATABRICKS_SCHEMA", ""),
                    help="UC schema name to drop (env: DATABRICKS_SCHEMA)")

    # -- cleanup-lakebase-schema ------------------------------------------
    lb = subparsers.add_parser("cleanup-lakebase-schema", help="Drop a Lakebase PG schema with CASCADE")
    lb.add_argument("--endpoint", default=os.environ.get("DATABRICKS_ENDPOINT", ""),
                    help="Databricks hostname for token generation (env: DATABRICKS_ENDPOINT)")
    lb.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""),
                    help="Databricks PAT for token generation (env: DATABRICKS_TOKEN)")
    lb.add_argument("--pg-host", default=os.environ.get("LAKEBASE_PG_HOST", ""),
                    help="Lakebase PG host (env: LAKEBASE_PG_HOST)")
    lb.add_argument("--pg-user", default=os.environ.get("LAKEBASE_PG_USER", ""),
                    help="Lakebase PG username (env: LAKEBASE_PG_USER)")
    lb.add_argument("--pg-db-name", default=os.environ.get("LAKEBASE_PG_DB_NAME", "spicebench"),
                    help="Lakebase PG database (env: LAKEBASE_PG_DB_NAME, default: spicebench)")
    lb.add_argument("--pg-schema", default=os.environ.get("LAKEBASE_PG_SCHEMA", ""),
                    help="Lakebase PG schema to drop (env: LAKEBASE_PG_SCHEMA)")
    lb.add_argument("--branch", default=os.environ.get("LAKEBASE_BRANCH", "production"),
                    help="Lakebase branch (env: LAKEBASE_BRANCH, default: production)")

    target = lb.add_mutually_exclusive_group()
    target.add_argument("--database-instance",
                        default=os.environ.get("LAKEBASE_DATABASE_INSTANCE", "") or None,
                        help="Provisioned instance name (env: LAKEBASE_DATABASE_INSTANCE)")
    target.add_argument("--project",
                        default=os.environ.get("LAKEBASE_PROJECT", "") or None,
                        help="Autoscaling project name (env: LAKEBASE_PROJECT)")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format=LOG_FORMAT,
    )

    try:
        if args.command == "cleanup-uc-schema":
            for field, label in [
                ("endpoint", "DATABRICKS_ENDPOINT"),
                ("token", "DATABRICKS_TOKEN"),
                ("warehouse_id", "DATABRICKS_SQL_WAREHOUSE_ID"),
                ("catalog", "DATABRICKS_CATALOG"),
                ("schema", "DATABRICKS_SCHEMA"),
            ]:
                if not getattr(args, field, ""):
                    parser.error(f"--{field.replace('_', '-')} (or env {label}) is required")
            cleanup_uc_schema(args)

        elif args.command == "cleanup-lakebase-schema":
            for field, label in [
                ("endpoint", "DATABRICKS_ENDPOINT"),
                ("token", "DATABRICKS_TOKEN"),
                ("pg_host", "LAKEBASE_PG_HOST"),
                ("pg_user", "LAKEBASE_PG_USER"),
                ("pg_schema", "LAKEBASE_PG_SCHEMA"),
            ]:
                if not getattr(args, field, ""):
                    parser.error(f"--{field.replace('_', '-')} (or env {label}) is required")

            if args.database_instance and args.project:
                parser.error("--database-instance and --project are mutually exclusive")
            if not args.database_instance and not args.project:
                parser.error(
                    "One of --database-instance (LAKEBASE_DATABASE_INSTANCE) or "
                    "--project (LAKEBASE_PROJECT) is required"
                )

            cleanup_lakebase_schema(args)

    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(130)
    except Exception as exc:
        logger.error("Fatal: %s", exc)
        if args.verbose:
            logger.debug("Traceback:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
