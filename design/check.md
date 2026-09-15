# `check` subcommand

## Goal

Give a user who cannot connect a single command that says which stage failed and why,
without ever revealing the password. Support engineers should be able to ask for the
output over chat.

## Stages

1. **Settings.** Print the resolved user, host:port, and database, and for each the source:
   `flag`, `env MMYSQL...`, or `default`. For the password print only that it is set, its
   length in characters, and warnings for leading/trailing whitespace, non-ASCII characters
   (smart quotes), control characters, and shell-special characters. `--fingerprint` adds
   the first eight hex characters of SHA-256 so two values can be compared safely.
2. **DNS.** Resolve the host. Failure here means a typo or missing DNS.
3. **TCP.** Plain dial with a five second timeout. A timeout points at a firewall; a refusal
   points at the server not listening on that address or port.
4. **Login.** Ping through the driver. Map common MySQL error numbers to hints:
   1045 password mismatch, 1130 host not allowed, 1044/1049 database problems,
   1251/2059 auth plugin, 3159 TLS required.
5. **Server.** On success print the version, `USER()` versus `CURRENT_USER()` so the user
   can see which account pattern matched, whether TLS is in use and with which cipher,
   and the selected database.

## Non-goals

- Never print the password, the DSN, or include the password in error text.
- Do not add diagnostics to the normal error path of other commands, so scripts logging
  stderr do not accumulate account details.
