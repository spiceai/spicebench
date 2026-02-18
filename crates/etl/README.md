# ETL Pipeline

```bash
cargo run -p etl -- --bucket peasee-indexes --region us-west-2 --source-prefix raw --dataset tpch --scale-factor 1.0 --num-steps 10 --adbc-driver databricks --adbc-uri "databricks://token:...@dbc-xxxx.cloud.databricks.com?http_path=..."
```