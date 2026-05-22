// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for the DynamoDB ADBC driver.
//!
//! Requires DynamoDB Local running at DYNAMODB_LOCAL_ENDPOINT (default: http://localhost:8000).
//!
//! Run DynamoDB Local:
//!   docker compose -f go/compose.yaml up -d
//!
//! Run tests:
//!   cargo test --test integration -- --ignored

use std::sync::Arc;
use std::time::Duration;

use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn dynamodb_endpoint() -> String {
    std::env::var("DYNAMODB_LOCAL_ENDPOINT").unwrap_or_else(|_| "http://localhost:8000".into())
}

fn dynamo_client() -> aws_sdk_dynamodb::Client {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_credential_types::Credentials::new(
                "fakeKey",
                "fakeSecret",
                None,
                None,
                "test",
            ))
            .load()
            .await;

        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::config::Builder::from(&config)
                .endpoint_url(dynamodb_endpoint())
                .build(),
        )
    })
}

fn is_dynamodb_available() -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = dynamo_client();
    rt.block_on(async { client.list_tables().send().await.is_ok() })
}

fn create_driver_and_db() -> impl Database {
    let mut driver = adbc_dynamodb::DynamoDBDriver::default();
    driver
        .new_database_with_opts(vec![
            (
                OptionDatabase::Other("adbc.driver.dynamodb.region".into()),
                OptionValue::String("us-east-1".into()),
            ),
            (
                OptionDatabase::Other("adbc.driver.dynamodb.endpoint".into()),
                OptionValue::String(dynamodb_endpoint()),
            ),
            (
                OptionDatabase::Other("adbc.driver.dynamodb.access_key_id".into()),
                OptionValue::String("fakeKey".into()),
            ),
            (
                OptionDatabase::Other("adbc.driver.dynamodb.secret_access_key".into()),
                OptionValue::String("fakeSecret".into()),
            ),
        ])
        .expect("failed to create database")
}

/// Create a DynamoDB table with the given name and partition key.
/// Waits for it to become active.
fn create_table(table_name: &str, pk: &str, sk: Option<&str>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = dynamo_client();
    rt.block_on(async {
        use aws_sdk_dynamodb::types::*;

        let mut key_schema = vec![KeySchemaElement::builder()
            .attribute_name(pk)
            .key_type(KeyType::Hash)
            .build()
            .unwrap()];

        let mut attr_defs = vec![AttributeDefinition::builder()
            .attribute_name(pk)
            .attribute_type(ScalarAttributeType::N)
            .build()
            .unwrap()];

        if let Some(sk_name) = sk {
            key_schema.push(
                KeySchemaElement::builder()
                    .attribute_name(sk_name)
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            );
            attr_defs.push(
                AttributeDefinition::builder()
                    .attribute_name(sk_name)
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            );
        }

        client
            .create_table()
            .table_name(table_name)
            .set_key_schema(Some(key_schema))
            .set_attribute_definitions(Some(attr_defs))
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .unwrap();

        // Wait for ACTIVE
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Ok(resp) = client.describe_table().table_name(table_name).send().await {
                if let Some(table) = resp.table() {
                    if table.table_status() == Some(&TableStatus::Active) {
                        break;
                    }
                }
            }
        }
    });
}

fn delete_table(table_name: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = dynamo_client();
    rt.block_on(async {
        let _ = client.delete_table().table_name(table_name).send().await;
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
                .is_err()
            {
                break;
            }
        }
    });
}

fn scan_table(table_name: &str) -> usize {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = dynamo_client();
    rt.block_on(async {
        let resp = client.scan().table_name(table_name).send().await.unwrap();
        resp.count() as usize
    })
}

fn build_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));

    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("item_{i}")).collect();
    let values: Vec<Option<f64>> = (0..num_rows)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(i as f64 * 1.5)
            }
        })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

// ── Ingest tests ────────────────────────────────────────────────

#[test]
#[ignore]
fn test_ingest_into_existing_table() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_ingest_basic";
    delete_table(table_name);
    create_table(table_name, "id", None);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_name.into()),
    )
    .unwrap();

    let batch = build_test_batch(10);
    let schema = batch.schema();
    let reader = Box::new(RecordBatchIterator::new(std::iter::once(Ok(batch)), schema));
    stmt.bind_stream(reader).unwrap();

    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, Some(10));

    // Verify via direct scan
    assert_eq!(scan_table(table_name), 10);

    delete_table(table_name);
}

#[test]
#[ignore]
fn test_ingest_into_empty_table() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_ingest_empty";
    delete_table(table_name);
    create_table(table_name, "id", None);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // Ingest into a table that exists but has zero rows
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_name.into()),
    )
    .unwrap();

    let batch = build_test_batch(5);
    stmt.bind(batch).unwrap();

    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, Some(5));
    assert_eq!(scan_table(table_name), 5);

    delete_table(table_name);
}

#[test]
#[ignore]
fn test_ingest_append_to_existing_data() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_ingest_append";
    delete_table(table_name);
    create_table(table_name, "id", None);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // First batch: 5 rows (ids 0-4)
    let mut stmt1 = conn.new_statement().unwrap();
    stmt1
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table_name.into()),
        )
        .unwrap();
    let batch1 = build_test_batch(5);
    stmt1.bind(batch1).unwrap();
    stmt1.execute_update().unwrap();

    // Second batch: 5 more rows (ids 5-9)
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table_name.into()),
        )
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5, 6, 7, 8, 9])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Float64Array::from(vec![5.0, 6.0, 7.0, 8.0, 9.0])),
        ],
    )
    .unwrap();
    stmt2.bind(batch2).unwrap();
    stmt2.execute_update().unwrap();

    assert_eq!(scan_table(table_name), 10);

    delete_table(table_name);
}

#[test]
#[ignore]
fn test_ingest_nonexistent_table_errors() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_ingest_noexist";
    delete_table(table_name);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_name.into()),
    )
    .unwrap();

    let batch = build_test_batch(5);
    stmt.bind(batch).unwrap();

    let result = stmt.execute_update();
    assert!(result.is_err(), "should fail for nonexistent table");
}

// ── SQL read tests ──────────────────────────────────────────────

#[test]
#[ignore]
fn test_sql_select() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_sql_select";
    delete_table(table_name);
    create_table(table_name, "id", None);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // Ingest data first
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_name.into()),
    )
    .unwrap();
    let batch = build_test_batch(10);
    stmt.bind(batch).unwrap();
    stmt.execute_update().unwrap();

    // Read back via SQL
    let mut read_stmt = conn.new_statement().unwrap();
    read_stmt
        .set_sql_query(&format!("SELECT * FROM {table_name}"))
        .unwrap();

    let reader = read_stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    delete_table(table_name);
}

#[test]
#[ignore]
fn test_sql_select_with_filter() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_sql_filter";
    delete_table(table_name);
    create_table(table_name, "id", None);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // Ingest
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_name.into()),
    )
    .unwrap();
    let batch = build_test_batch(20);
    stmt.bind(batch).unwrap();
    stmt.execute_update().unwrap();

    // Query with filter
    let mut read_stmt = conn.new_statement().unwrap();
    read_stmt
        .set_sql_query(&format!("SELECT * FROM {table_name} WHERE id < 5"))
        .unwrap();

    let reader = read_stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    delete_table(table_name);
}

// ── Metadata tests ──────────────────────────────────────────────

#[test]
#[ignore]
fn test_get_table_schema() {
    if !is_dynamodb_available() {
        return;
    }

    let table_name = "test_rust_schema";
    delete_table(table_name);
    create_table(table_name, "id", None);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // Ingest so table has data for schema inference
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_name.into()),
    )
    .unwrap();
    let batch = build_test_batch(3);
    stmt.bind(batch).unwrap();
    stmt.execute_update().unwrap();

    // Get schema
    let schema = conn.get_table_schema(None, None, table_name).unwrap();
    assert!(!schema.fields().is_empty());

    delete_table(table_name);
}

#[test]
#[ignore]
fn test_get_table_types() {
    if !is_dynamodb_available() {
        return;
    }

    let db = create_driver_and_db();
    let conn = db.new_connection().unwrap();

    let reader = conn.get_table_types().unwrap();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

// ── Error handling tests ────────────────────────────────────────

#[test]
#[ignore]
fn test_execute_without_query_errors() {
    if !is_dynamodb_available() {
        return;
    }

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();

    let result = stmt.execute();
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_execute_update_without_data_errors() {
    if !is_dynamodb_available() {
        return;
    }

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("some_table".into()),
    )
    .unwrap();

    let result = stmt.execute_update();
    assert!(result.is_err(), "should fail without bound data");
}

// ── Non-DynamoDB unit-level tests ───────────────────────────────

#[test]
fn test_database_options() {
    let mut driver = adbc_dynamodb::DynamoDBDriver::default();
    let db = driver
        .new_database_with_opts(vec![(
            OptionDatabase::Other("adbc.driver.dynamodb.region".into()),
            OptionValue::String("eu-west-1".into()),
        )])
        .unwrap();

    let region = db
        .get_option_string(OptionDatabase::Other("adbc.driver.dynamodb.region".into()))
        .unwrap();
    assert_eq!(region, "eu-west-1");
}

#[test]
fn test_statement_options() {
    let mut driver = adbc_dynamodb::DynamoDBDriver::default();
    let db = driver.new_database().unwrap();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("my_table".into()),
    )
    .unwrap();
    assert_eq!(
        stmt.get_option_string(OptionStatement::TargetTable)
            .unwrap(),
        "my_table"
    );

    stmt.set_option(
        OptionStatement::Other("adbc.driver.dynamodb.parallelism".into()),
        OptionValue::String("25".into()),
    )
    .unwrap();

    // Invalid parallelism
    let result = stmt.set_option(
        OptionStatement::Other("adbc.driver.dynamodb.parallelism".into()),
        OptionValue::String("not_a_number".into()),
    );
    assert!(result.is_err());
}

#[test]
fn test_per_table_parallelism() {
    let mut driver = adbc_dynamodb::DynamoDBDriver::default();
    let db = driver
        .new_database_with_opts(vec![
            (
                OptionDatabase::Other("adbc.driver.dynamodb.parallelism".into()),
                OptionValue::String("10".into()),
            ),
            (
                OptionDatabase::Other("adbc.driver.dynamodb.parallelism.orders".into()),
                OptionValue::String("50".into()),
            ),
        ])
        .unwrap();

    // Verify the options were accepted (no error)
    let _ = db.new_connection().unwrap();
}
