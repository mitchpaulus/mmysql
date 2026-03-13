# `insert` subcommand

I want a simplified insert experience.
The CLI should take the insert data as JSON.
The JSON must be an array of dictionaries, or a single dictionary. Anything else should be a hard fail and reported to the user.

Put all the inserts together, and wrap into a single transaction, so if something fails, the whole thing is aborted.

```
mmysql insert mytable '{ 'col1': 'value', 'col2': 'value3' }
```

There should also be an '--ignore' flag that would add the 'IGNORE' parameter to the insert query.
