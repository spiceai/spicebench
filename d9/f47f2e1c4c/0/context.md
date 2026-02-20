# Session Context

## User Prompts

### Prompt 1

Try and figur eout why im not seeing an eprintln of "[Spicebench] helllo"

### Prompt 2

where would adapter.lock().await be getting  blocked by

### Prompt 3

can you add eprintln!( statements to /Users/jeadie/Github/spicebench-2/src/commands/load/mod.rs to help debug this

### Prompt 4

not seeing anything, i even added some more

### Prompt 5

~/.spice/bin/spicebench \
    --concurrency 1 \
    --scenario tpch \
    --executor-instance-type "local" \
    --etl-bucket spiceai-public-datasets \
    --etl-region us-east-1 --etl-prefix data-gen \
    --system-adapter-stdio-cmd ~/.spice/bin/spidapter \
    --system-adapter-stdio-args "stdio --verbose --channel nightly" --scrape-sut-metrics --etl-version 3

