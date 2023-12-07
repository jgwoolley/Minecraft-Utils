# Minecraft Utilities

[See PyDocs](https://jgwoolley.github.io/Minecraft-Utils/utils.html)

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
FROM LOGS_LOGGED_IN
;
```

```sql
SELECT
	LOGS_LOGGED_IN.player,
	LOGS_LOGGED_IN.log_datetime,
	MINECRAFT_SERVER_REGIONS.region_x,
	MINECRAFT_SERVER_REGIONS.region_z,
	LOGS_LOGGED_IN.x,
	LOGS_LOGGED_IN.y,
	LOGS_LOGGED_IN.z
FROM LOGS_LOGGED_IN

LEFT JOIN MINECRAFT_SERVER_REGIONS ON
	MINECRAFT_SERVER_REGIONS.run_date = LOGS_LOGGED_IN.run_date AND
	MINECRAFT_SERVER_REGIONS.min_x < LOGS_LOGGED_IN.x AND
	MINECRAFT_SERVER_REGIONS.max_x > LOGS_LOGGED_IN.x AND
	MINECRAFT_SERVER_REGIONS.min_y < LOGS_LOGGED_IN.y AND
	MINECRAFT_SERVER_REGIONS.max_y > LOGS_LOGGED_IN.y AND
	MINECRAFT_SERVER_REGIONS.min_z < LOGS_LOGGED_IN.z AND
	MINECRAFT_SERVER_REGIONS.max_z > LOGS_LOGGED_IN.z
;
```

## TODO

- LOGS_MOVED_TOO_QUICKLY has coordinates, but what do they mean??? They don't look like regular coords. Maybe chunk?
