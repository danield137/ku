# ku

Slim Kusto CLI — query Azure Data Explorer from the command line and save results as KDF files.

No Kusto SDK. No heavy dependencies. Just HTTP + JSON + Azure CLI auth.

## Install

### From crates.io (requires Rust toolchain)

```bash
cargo install kusto-query
```

### From source

```bash
git clone https://github.com/danield137/ku.git
cd ku
cargo install --path .
```

### Prebuilt binaries

Download from [GitHub Releases](https://github.com/danield137/ku/releases) — available for Linux, macOS, and Windows.

## Usage

```
ku --cluster <url> --database <db> --query <kql> --output <file.kdf>
ku --cluster <url> --database <db> --query <kql> --adaptive-output <file.kdf>
```

### Options

| Flag | Description |
|------|-------------|
| `--cluster <url>` | Kusto cluster URL |
| `--database <db>` | Database name |
| `--query <kql>` | KQL query |
| `--output <file>` | Always write KDF file |
| `--adaptive-output <file>` | Write KDF only if result is large; print small results inline |
| `--head <N>` | Print first N rows (default: 5 with `--adaptive-output`) |
| `-pp`, `--pretty-print` | Use padded table format instead of CSV |
| `-h`, `--help` | Print help |

### Examples

Query and save to file:

```bash
ku --cluster https://help.kusto.windows.net --database Samples \
   --query "StormEvents | count" --output result.kdf
```

Adaptive output — small results print inline, large ones save to file:

```bash
ku --cluster https://help.kusto.windows.net --database Samples \
   --query "StormEvents | summarize count() by State" --adaptive-output states.kdf
```

Preview first 3 rows with pretty-print:

```bash
ku --cluster https://help.kusto.windows.net --database Samples \
   --query "StormEvents | take 100" --output events.kdf --head 3 -pp
```

## Requirements

- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) — `ku` uses `az account get-access-token` for authentication
- You must be logged in: `az login`

## KDF Format

KDF (Kusto Data Frame) is a JSON file containing the Kusto V2 PrimaryResult frame. It preserves column names, types, and all row data exactly as returned by Kusto.

## License

MIT
