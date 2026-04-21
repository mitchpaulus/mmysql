# `execute` subcommand

## Goal

Provide an explicit subcommand for "run this SQL" without making the user think about query shape.

This should support:

- `SELECT` and other row-returning statements
- `INSERT`, `UPDATE`, `DELETE`, DDL, and other non-row-returning statements
- query text from CLI arguments or stdin

The command should stay minimal and keep JSON as the default output format.

## Proposed CLI

```sh
mmysql execute select '*' from users where id = 1
mmysql ex select '*' from users where id = 1
echo "update users set enabled = 0 where last_login < '2024-01-01'" | mmysql execute
```

Add `ex` as a supported shorthand alias for `execute`.

## Query input

Input rules:

- If positional arguments are present, join them with spaces and use that as the SQL text.
- Otherwise read stdin.
- If stdin is a terminal and no SQL was provided, fail with a clear usage message.
- Trim leading and trailing whitespace before execution.
- If stdin is not a terminal, read it and fail if the trimmed SQL is empty.

This preserves the convenience of:

```sh
mmysql execute select id, name from users where id = 1
```

without requiring quotes for simple one-line statements.

## TTY behavior

Use `os.Stdin.Stat()` and check `os.ModeCharDevice`.

Cases:

- args present:
  - join args with spaces
  - ignore stdin
- no args and stdin is not a TTY:
  - read stdin
  - trim whitespace
  - fail on empty query
- no args and stdin is a TTY:
  - fail immediately
  - print guidance showing both supported forms

Suggested guidance:

```text
error: no query provided
usage: mmysql execute [options] <query>
       mmysql ex [options] <query>
       echo 'SELECT 1' | mmysql execute [options]
```

This is stricter than the current behavior and matches the requirement to fail on empty query with TTY stdin.

## Execution model

Run the SQL inside a single transaction by default.

That keeps behavior aligned with the project design notes and gives predictable semantics for write queries.

Flow:

1. Open DB connection.
2. Begin transaction.
3. Attempt `QueryContext`.
4. If the statement returns rows, scan them and commit after rows are fully consumed.
5. If the driver reports that the statement does not return rows, fall back to `ExecContext`.
6. Commit on success, rollback on any error.

## Output shape

JSON should be the only initial output format.

For row-returning queries:

```json
[
  {
    "id": 1,
    "name": "Ada"
  }
]
```

For non-row-returning queries:

```json
{
  "rows_affected": 3
}
```

For statements where the driver exposes it, it would also be reasonable to include:

```json
{
  "rows_affected": 1,
  "last_insert_id": 42
}
```

I would treat `last_insert_id` as optional, not guaranteed.

## Recommended initial flags

Include:

- `-n, --dry-run` to print the final SQL text without executing it

For `execute`, dry-run is mainly useful for:

- confirming what was read from stdin
- confirming how space-joined CLI arguments resolved into a query
- debugging shell quoting issues

Because there are no bound parameters in this command, dry-run should print the final query text as-is.

Do not add `--no-tx` in the first pass.

## Error handling

Errors should continue to go to stderr in the existing `error: ...` style.

Important cases:

- missing SQL
- empty stdin
- SQL execution error
- scan/encoding failure
- commit/rollback failure

## Implementation plan

1. Extract query-text parsing into a helper for `execute`.
2. Replace the current `db.Query(...)` only flow with transaction-aware execute logic that supports both `Query` and `Exec`.
3. Remove the implicit bare-query command path from `main()`.
4. Add `execute` and `ex` to the top-level command switch.
5. Update usage text in `main.go` so `execute` is the only query-running entrypoint.
6. Add `-n, --dry-run`.
7. Add tests for:
   - query from args
   - query from stdin
   - row-returning statement output
   - non-row-returning statement output
   - empty query failure from piped stdin
   - no query failure with TTY stdin
   - dry-run output

## Design call

The main design choice is whether `execute` should try to infer query type up front.

It should not.

Avoid SQL parsing and let the driver/database decide.
The command should attempt execution and adapt based on whether rows are returned.
That is simpler, more robust, and consistent with the tool's overall design goals.
