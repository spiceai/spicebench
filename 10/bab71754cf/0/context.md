# Session Context

## User Prompts

### Prompt 1

Error: Failed to create ADBC connection for driver databricks: Failed to create database handle: InvalidArguments: cannot specify both URI and individual connection options (sqlstate: [0, 0, 0, 0, 0], vendor_code: -2147483648)


Because in `system-adapters/databricks/src/main.rs` `fn setup`, we use both `uri` and `catalog` and `schema` as options. catalog and schema must be in uri query path. 

databricks://token:<personal-access-token>@<server-hostname>:<port-number>/<http-path>?<param1=value1>...

