# Manual scripts

The obsolete root-level API/export smoke scripts were removed because they duplicated the CLI,
contained unsafe token handling, and called async synchronization without awaiting it. Use the
supported commands instead:

```bash
uv run zepp-life-mcp doctor
uv run zepp-life-mcp sync
```
