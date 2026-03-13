Design considerations:

- JSON by default
- By default, pull authentication from Environment Variables, able to override by CLI options:
  - $MMYSQLUSER
  - $MMYSQLPASSWORD
  - $MMYSQLHOST
  - $MMYSQLDATABASE
- Assume UTF-8 by default for everything
- Target Linux and Windows.
- When executing, everything should also be wrapped into a single transaction.
