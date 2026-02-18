# ETL Pipeline

```bash
cargo run -p etl -- --bucket peasee-indexes --region us-west-2 --source-prefix raw --target-prefix rehydrated --dataset tpch --scale-factor 1.0 --num-steps 10
```