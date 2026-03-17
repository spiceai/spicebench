# Data Generator

Generate versioned dataset archives and upload them to S3 or write them locally.

This crate provides the `data-generation` library used by the `spicebench generate` subcommand.

## Example

```bash
spicebench generate \
    --scale-factor 1 \
    --bucket my-benchmark-data \
    --region us-west-2 \
    --prefix raw \
    --num-steps 10
```

To write a local archive instead of uploading to S3, add `--output-archive`:
```bash
cargo run -p data-generation -- run 
  --scenario tpch 
  --scale-factor 1 
  --num-steps 20 
  --output-archive ./my-tpch.tar.zst
```
