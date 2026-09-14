# mmysql

A small cross-platform CLI for querying and modifying MySQL databases, with JSON output by default so results are easy to pipe into other tools.

## Install

**Linux / macOS**

```sh
curl -fsSL https://raw.githubusercontent.com/mitchpaulus/mmysql/main/install.sh | sh
```

Installs to `~/.local/bin`. Set `MMYSQL_INSTALL_DIR` to change the location.

**Windows (PowerShell)**

```powershell
irm https://raw.githubusercontent.com/mitchpaulus/mmysql/main/install.ps1 | iex
```

Installs to `%LOCALAPPDATA%\Programs\mmysql` and adds it to your user PATH. Set `$env:MMYSQL_INSTALL_DIR` to change the location.

**Updating**

Re-run the same install command. To install a specific version, set `MMYSQL_VERSION` (for example `v1.2.0`) before running it.

**Manual**

Prebuilt binaries for Linux, macOS, and Windows are on the [releases page](https://github.com/mitchpaulus/mmysql/releases).
Download the one for your platform and put it on your PATH.

## Usage

```
mmysql <command> [options]

Commands:
  execute   Execute a SQL query
  ex        Shorthand for execute
  insert    Insert JSON data into a table
  upsert    Insert or update JSON data in a table
  update    Update rows matching key columns
  version   Print version and exit
```

Connection settings come from environment variables and can be overridden with flags:

| Flag               | Environment variable |
| ------------------ | -------------------- |
| `-u`, `--user`     | `MMYSQLUSER`         |
| `-p`, `--password` | `MMYSQLPASSWORD`     |
| `-H`, `--host`     | `MMYSQLHOST`         |
| `-d`, `--database` | `MMYSQLDATABASE`     |

The host defaults to `localhost` and the port to `3306`. All connections use `utf8mb4`.

Output is JSON by default. Pass `--csv` or `--tsv` for delimited output instead.

```sh
mmysql ex "SELECT id, name FROM users LIMIT 5"
mmysql ex --csv "SELECT * FROM orders" > orders.csv
echo '{"id": 1, "name": "Ada"}' | mmysql insert users
```

## License

See [LICENSE](LICENSE).
