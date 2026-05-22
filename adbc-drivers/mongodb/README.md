# ADBC MongoDB Driver

An [ADBC](https://arrow.apache.org/adbc/) driver for MongoDB, enabling Arrow-native reads and writes.

## Rust Driver

Uses [DataFusion](https://datafusion.apache.org/) for SQL parsing and the [datafusion-table-providers MongoDB provider](https://github.com/datafusion-contrib/datafusion-table-providers) for MongoDB I/O.

### Features

- **Read**: SQL queries via DataFusion (`SELECT`, filter pushdown)
- **Write**: Bulk ingest via `MongoDBTableFactory::insert_into` (lazy streaming, no in-memory buffering)
- **Shared library**: Exports `AdbcDriverMongodbInit` C entry point for cross-language use

### Build

Requires Rust 1.83.0+.

```bash
cd rust
cargo build --release
```

The shared library is at `target/release/libadbc_mongodb.dylib` (macOS) or `.so` (Linux).

### Configuration

#### Database options

| Option | Description | Default |
|--------|-------------|---------|
| `adbc.driver.mongodb.connection_string` | Full MongoDB URI (`mongodb://` or `mongodb+srv://`) | - |
| `adbc.driver.mongodb.host` | MongoDB host | `localhost` |
| `adbc.driver.mongodb.port` | MongoDB port | `27017` |
| `adbc.driver.mongodb.db` | Database name | - |
| `adbc.driver.mongodb.user` | Username | - |
| `adbc.driver.mongodb.pass` | Password | - |
| `adbc.driver.mongodb.auth_source` | Authentication database | - |
| `adbc.driver.mongodb.srv` | Use SRV DNS record (`true`/`false`) | `false` |
| `adbc.driver.mongodb.direct_connection` | Force direct connection (`true`/`false`) | `false` |
| `adbc.driver.mongodb.sslmode` | TLS mode (`disable`, `require`) | - |
| `adbc.driver.mongodb.sslrootcert` | Path to CA certificate file | - |
| `adbc.driver.mongodb.pool_min` | Minimum connection pool size | - |
| `adbc.driver.mongodb.pool_max` | Maximum connection pool size | - |
| `adbc.driver.mongodb.time_zone` | Time zone for datetime parsing | - |
| `adbc.driver.mongodb.unnest_depth` | Depth to unnest nested documents | - |
| `adbc.driver.mongodb.schema_infer_max_records` | Records sampled for schema inference | - |

`OptionDatabase::Uri` is also accepted as a MongoDB connection string.

#### Statement options

| Option | Description |
|--------|-------------|
| `adbc.ingest.target_table` | Target MongoDB collection for bulk ingest |
| `adbc.ingest.mode` | `append` (default) or `replace` |

### Usage (Rust)

```rust
use adbc_core::{Driver, Database, Connection, Statement, Optionable};
use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};

let mut driver = adbc_mongodb::MongoDBDriver::default();
let db = driver.new_database_with_opts(vec![
    (OptionDatabase::Uri, OptionValue::String("mongodb://localhost:27017".into())),
])?;

let mut conn = db.new_connection()?;

// Read
let mut stmt = conn.new_statement()?;
stmt.set_sql_query("SELECT * FROM my_collection WHERE status = 'active'")?;
let reader = stmt.execute()?;

// Write
let mut stmt = conn.new_statement()?;
stmt.set_option(OptionStatement::TargetTable, OptionValue::String("my_collection".into()))?;
stmt.bind(record_batch)?;
let rows = stmt.execute_update()?;
```

### Usage (Python via driver manager)

```python
import adbc_driver_manager

db = adbc_driver_manager.AdbcDatabase(
    driver="path/to/libadbc_mongodb.dylib",
    entrypoint="AdbcDriverMongodbInit",
    uri="mongodb://localhost:27017",
)
conn = db.adbc_connection()
```

## License

Apache License 2.0
