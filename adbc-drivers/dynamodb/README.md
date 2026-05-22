# ADBC DynamoDB Driver

An [ADBC](https://arrow.apache.org/adbc/) driver for Amazon DynamoDB, enabling Arrow-native reads and writes.

## Rust Driver

Uses [DataFusion](https://datafusion.apache.org/) for SQL parsing and the [spiceai DynamoDB TableProvider](https://github.com/spiceai/spiceai/tree/trunk/crates/data_components/src/dynamodb) for DynamoDB I/O.

### Features

- **Read**: SQL queries via DataFusion (`SELECT`, filter pushdown, parallel scan)
- **Write**: Bulk ingest via `DynamoDBTableProvider::insert_into` (`BatchWriteItem`, configurable parallelism)
- **Delete**: SQL `DELETE FROM` via `DynamoDBDeletionSink`
- **Shared library**: Exports `AdbcDriverDynamodbInit` C entry point for cross-language use

### Build

Requires Rust 1.93.1+.

```bash
cd rust
cargo build --release
```

The shared library is at `target/release/libadbc_dynamodb.dylib` (macOS) or `.so` (Linux).

### Configuration

#### Database options

| Option | Description | Default |
|--------|-------------|---------|
| `adbc.driver.dynamodb.region` | AWS region | `us-east-1` |
| `adbc.driver.dynamodb.endpoint` | Custom endpoint (e.g. DynamoDB Local) | - |
| `adbc.driver.dynamodb.access_key_id` | AWS access key ID | default chain |
| `adbc.driver.dynamodb.secret_access_key` | AWS secret access key | default chain |
| `adbc.driver.dynamodb.session_token` | AWS session token | - |
| `adbc.driver.dynamodb.profile` | AWS config profile name | - |
| `adbc.driver.dynamodb.parallelism` | Default write parallelism | `10` |
| `adbc.driver.dynamodb.parallelism.<table>` | Per-table write parallelism | - |

#### Statement options

| Option | Description |
|--------|-------------|
| `adbc.ingest.target_table` | Target DynamoDB table for bulk ingest |
| `adbc.ingest.mode` | Ingest mode (accepted, currently always appends) |
| `adbc.driver.dynamodb.parallelism` | Per-statement parallelism override |

### Usage (Rust)

```rust
use adbc_core::{Driver, Database, Connection, Statement, Optionable};
use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};

let mut driver = adbc_dynamodb::DynamoDBDriver::default();
let db = driver.new_database_with_opts(vec![
    (OptionDatabase::Other("adbc.driver.dynamodb.region".into()), OptionValue::String("us-west-2".into())),
])?;

let mut conn = db.new_connection()?;

// Read
let mut stmt = conn.new_statement()?;
stmt.set_sql_query("SELECT * FROM my_table WHERE id = '123'")?;
let reader = stmt.execute()?;

// Write
let mut stmt = conn.new_statement()?;
stmt.set_option(OptionStatement::TargetTable, OptionValue::String("my_table".into()))?;
stmt.bind(record_batch)?;
let rows = stmt.execute_update()?;
```

### Usage (Python via driver manager)

```python
import adbc_driver_manager

db = adbc_driver_manager.AdbcDatabase(
    driver="path/to/libadbc_dynamodb.dylib",
    entrypoint="AdbcDriverDynamodbInit",
)
db.set_options({
    "adbc.driver.dynamodb.region": "us-west-2",
})
conn = db.adbc_connection()
```

## License

Apache License 2.0
