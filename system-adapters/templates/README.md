# System Adapter Templates

This folder provides starter templates for building custom SpiceBench system adapters:

- [Python template](./python/README.md)
- [Node.js template](./nodejs/README.md)
- [Rust template](./rust/README.md)
- [Go template](./go/README.md)
- [Java template](./java/README.md)

All templates:

- implement JSON-RPC 2.0 methods `setup`, `teardown`, `metrics`, and `rpc.methods`
- support JSON-RPC over stdio (line-delimited requests)
- support JSON-RPC over HTTP (POST endpoint, default `/jsonrpc`)
- include `metrics` stubs with commented examples of where to poll/monitor your SUT
- are intentionally minimal and designed for customization

Runtime targets used by these templates and CI checks:

- Python: latest `3.x`
- Node.js: latest LTS (`lts/*`)
- Rust: current stable toolchain
- Go: `1.26`
- Java: latest LTS (`25`)

Claude skill for adapter authoring:

- `.claude/skills/system-adapter-builder/SKILL.md`
