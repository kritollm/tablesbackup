# Tablesbackup

Because of lack of a good solution for backing up azure tables I created this script.
(Could have been made more elegant with async await and [callBackToPromiseWrapper](https://gist.github.com/kritollm/816b77e0537ff4a13d2f0ba7d1006952).

Just store a creds.json file like this:

```jascript
  {
    "account": "yourAccount",
    "accountKey": "yourAccountKey==",
    "maxTablesSimultaneous": 10,
    "maxEntitiesInFile": 2000,
    "savePath": "safetyBackupTables/"
  }
```
and in the terminal type ```npm start``` or ```node app```.

The next time it will only add changes. It will also catch changes stored in the same millisecond as the last entity.
