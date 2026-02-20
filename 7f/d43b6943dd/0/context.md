# Session Context

## User Prompts

### Prompt 1

remove any reference to create_tables RPC in crates/system-adapter-protocol

### Prompt 2

[Request interrupted by user]

### Prompt 3

Compiling checkpointer v0.1.0 (/Users/jeadie/Github/spicebench-2/crates/checkpointer)
   Compiling spicebench v0.1.0 (/Users/jeadie/Github/spicebench-2)
error[E0599]: no method named `create_tables` found for struct `tokio::sync::MutexGuard<'_, system_adapter_protocol::Client>` in the current scope
   --> src/main.rs:141:10
    |
138 | /     system_adapter_client
139 | |         .lock()
140 | |         .await
141 | |         .create_tables(run_id, datasets)
    | |         -^^^^^^^^^^^^^ method ...

