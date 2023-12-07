# Minecraft Utilities

Running [utils.py](utils.py) will produce a sqlite3 database with information on a Minecraft server, primarily based on log files.

``` console
python utils.py --help
usage: MinecraftLogParser [-h] [-i INPUT] [-o OUTPUT]

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
  -o OUTPUT, --output OUTPUT
```

## Some example SQL querries

```sql
SELECT DISTINCT
	LOGS_LOGGED_IN.IP,
	LOGS_LOGGED_IN.player
FROM LOGS_LOGGED_IN;
```

## TODO

- LOGS_MOVED_TOO_QUICKLY has coordinates, but what do they mean??? They don't look like regular coords. Maybe chunk?
- Have the documentation auto generate?

``` console
python -m pydoc -w util
wrote utils.html
```