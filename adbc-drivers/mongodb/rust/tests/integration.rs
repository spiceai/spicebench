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

//! Integration tests for the MongoDB ADBC driver.
//!
//! Requires a MongoDB instance running at MONGODB_URI (default: mongodb://localhost:27017).
//!
//! Run MongoDB locally:
//!   docker run -d -p 27017:27017 mongo:7
//!
//! Run tests:
//!   cargo test --test integration -- --ignored

use std::sync::Arc;

use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn mongodb_uri() -> String {
    std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

fn mongodb_db() -> String {
    std::env::var("MONGODB_DB").unwrap_or_else(|_| "adbc_test".into())
}

fn is_mongodb_available() -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = mongodb::Client::with_uri_str(&mongodb_uri()).await;
        match client {
            Ok(c) => c
                .database("admin")
                .run_command(mongodb::bson::doc! { "ping": 1 })
                .await
                .is_ok(),
            Err(_) => false,
        }
    })
}

fn drop_collection(collection: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = mongodb::Client::with_uri_str(&mongodb_uri()).await.unwrap();
        let _ = client
            .database(&mongodb_db())
            .collection::<mongodb::bson::Document>(collection)
            .drop()
            .await;
    });
}

fn count_documents(collection: &str) -> u64 {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = mongodb::Client::with_uri_str(&mongodb_uri()).await.unwrap();
        client
            .database(&mongodb_db())
            .collection::<mongodb::bson::Document>(collection)
            .count_documents(mongodb::bson::doc! {})
            .await
            .unwrap_or(0)
    })
}

fn create_driver_and_db() -> impl Database {
    // Embed db name and tls=false in the URI. Using connection_string + db as
    // separate params doesn't work: build_connection_uri ignores `db` when
    // connection_string is present, and DEFAULT_SSL_MODE is "required" so TLS
    // must be explicitly disabled for a plain-TCP local server.
    let base = mongodb_uri();
    let sep = if base.contains('?') { "&" } else { "?" };
    let uri = format!("{}/{}{sep}tls=false", base.trim_end_matches('/'), mongodb_db());

    let mut driver = adbc_mongodb::MongoDBDriver::default();
    driver
        .new_database_with_opts(vec![(
            OptionDatabase::Uri,
            OptionValue::String(uri),
        )])
        .expect("failed to create database")
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

// ── Ingest tests ─────────────────────────────────────────────────

#[test]
#[ignore]
fn test_ingest_into_new_collection() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_ingest_basic";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(coll.into()),
    )
    .unwrap();

    let batch = build_test_batch(10);
    let schema = batch.schema();
    let reader = Box::new(RecordBatchIterator::new(std::iter::once(Ok(batch)), schema));
    stmt.bind_stream(reader).unwrap();

    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, Some(10));
    assert_eq!(count_documents(coll), 10);

    drop_collection(coll);
}

#[test]
#[ignore]
fn test_ingest_append_to_existing_data() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_ingest_append";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // First batch: 5 rows
    let mut stmt1 = conn.new_statement().unwrap();
    stmt1
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(coll.into()),
        )
        .unwrap();
    stmt1.bind(build_test_batch(5)).unwrap();
    stmt1.execute_update().unwrap();

    // Second batch: 5 more rows
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(coll.into()),
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

    assert_eq!(count_documents(coll), 10);

    drop_collection(coll);
}

#[test]
#[ignore]
fn test_ingest_overwrite_mode() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_ingest_overwrite";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // Insert 10 rows
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(coll.into()),
    )
    .unwrap();
    stmt.bind(build_test_batch(10)).unwrap();
    stmt.execute_update().unwrap();
    assert_eq!(count_documents(coll), 10);

    // Overwrite with 3 rows
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(coll.into()),
        )
        .unwrap();
    stmt2
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("replace".into()),
        )
        .unwrap();
    stmt2.bind(build_test_batch(3)).unwrap();
    stmt2.execute_update().unwrap();
    assert_eq!(count_documents(coll), 3);

    drop_collection(coll);
}

// ── SQL read tests ───────────────────────────────────────────────

#[test]
#[ignore]
fn test_sql_select() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_sql_select";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    // Ingest data first
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(coll.into()),
    )
    .unwrap();
    stmt.bind(build_test_batch(10)).unwrap();
    stmt.execute_update().unwrap();

    // Read back via SQL
    let mut read_stmt = conn.new_statement().unwrap();
    read_stmt
        .set_sql_query(&format!("SELECT * FROM {coll}"))
        .unwrap();

    let reader = read_stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    drop_collection(coll);
}

#[test]
#[ignore]
fn test_sql_select_with_filter() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_sql_filter";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(coll.into()),
    )
    .unwrap();
    stmt.bind(build_test_batch(20)).unwrap();
    stmt.execute_update().unwrap();

    let mut read_stmt = conn.new_statement().unwrap();
    read_stmt
        .set_sql_query(&format!("SELECT * FROM {coll} WHERE id < 5"))
        .unwrap();

    let reader = read_stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    drop_collection(coll);
}

// ── Metadata tests ───────────────────────────────────────────────

#[test]
#[ignore]
fn test_get_table_schema() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_schema";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(coll.into()),
    )
    .unwrap();
    stmt.bind(build_test_batch(5)).unwrap();
    stmt.execute_update().unwrap();

    let schema = conn.get_table_schema(None, None, coll).unwrap();
    assert!(!schema.fields().is_empty());

    drop_collection(coll);
}

#[test]
#[ignore]
fn test_get_table_types() {
    if !is_mongodb_available() {
        return;
    }

    let db = create_driver_and_db();
    let conn = db.new_connection().unwrap();

    let reader = conn.get_table_types().unwrap();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

// ── Error handling tests ─────────────────────────────────────────

#[test]
#[ignore]
fn test_execute_without_query_errors() {
    if !is_mongodb_available() {
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
    if !is_mongodb_available() {
        return;
    }

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("some_collection".into()),
    )
    .unwrap();

    let result = stmt.execute_update();
    assert!(result.is_err(), "should fail without bound data");
}

// ── Multi-batch stream test ──────────────────────────────────────

#[test]
#[ignore]
fn test_ingest_multi_batch_stream() {
    if !is_mongodb_available() {
        return;
    }

    let coll = "test_adbc_ingest_multi_batch";
    drop_collection(coll);

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));

    // Build 3 separate batches and wrap them in a single stream
    let batches = vec![
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![0.0, 1.5, 3.0])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![3, 4, 5])),
                Arc::new(StringArray::from(vec!["d", "e", "f"])),
                Arc::new(Float64Array::from(vec![4.5, 6.0, 7.5])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![6, 7, 8, 9])),
                Arc::new(StringArray::from(vec!["g", "h", "i", "j"])),
                Arc::new(Float64Array::from(vec![9.0, 10.5, 12.0, 13.5])),
            ],
        )
        .unwrap(),
    ];

    let reader = Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        Arc::clone(&schema),
    ));

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(coll.into()),
    )
    .unwrap();
    stmt.bind_stream(reader).unwrap();

    let rows = stmt.execute_update().unwrap();
    assert_eq!(rows, Some(10));
    assert_eq!(count_documents(coll), 10);

    drop_collection(coll);
}

// ── Unit tests (no MongoDB required) ────────────────────────────

#[test]
fn test_database_options() {
    let mut driver = adbc_mongodb::MongoDBDriver::default();
    // Only option parsing is tested here — no connection is opened
    let result = driver.new_database_with_opts(vec![
        (
            OptionDatabase::Other("adbc.driver.mongodb.host".into()),
            OptionValue::String("myhost".into()),
        ),
        (
            OptionDatabase::Other("adbc.driver.mongodb.port".into()),
            OptionValue::String("27017".into()),
        ),
        (
            OptionDatabase::Other("adbc.driver.mongodb.db".into()),
            OptionValue::String("mydb".into()),
        ),
    ]);
    assert!(
        result.is_ok(),
        "database creation with valid options should succeed"
    );

    let db = result.unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other("adbc.driver.mongodb.host".into()))
            .unwrap(),
        "myhost"
    );
}

#[test]
fn test_database_uri_option() {
    let mut driver = adbc_mongodb::MongoDBDriver::default();
    let result = driver.new_database_with_opts(vec![(
        OptionDatabase::Uri,
        OptionValue::String("mongodb://localhost:27017/mydb".into()),
    )]);
    assert!(result.is_ok());
}

#[test]
fn test_invalid_pool_min_errors() {
    let mut driver = adbc_mongodb::MongoDBDriver::default();
    let result = driver.new_database_with_opts(vec![(
        OptionDatabase::Other("adbc.driver.mongodb.pool_min".into()),
        OptionValue::String("not_a_number".into()),
    )]);
    assert!(result.is_err(), "invalid pool_min should fail");
}

#[test]
#[ignore]
fn test_statement_options() {
    if !is_mongodb_available() {
        return;
    }

    let db = create_driver_and_db();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("my_collection".into()),
    )
    .unwrap();
    assert_eq!(
        stmt.get_option_string(OptionStatement::TargetTable).unwrap(),
        "my_collection"
    );

    stmt.set_option(
        OptionStatement::IngestMode,
        OptionValue::String("replace".into()),
    )
    .unwrap();
    assert_eq!(
        stmt.get_option_string(OptionStatement::IngestMode).unwrap(),
        "replace"
    );

    // IngestMode defaults to empty string when unset
    let stmt2 = conn.new_statement().unwrap();
    assert_eq!(
        stmt2
            .get_option_string(OptionStatement::IngestMode)
            .unwrap(),
        ""
    );
}
