'''
This is where the module documentation goes 
'''

import pathlib, gzip, datetime, json, re, argparse, sqlite3, hashlib
from io import TextIOWrapper
from typing import Union, Text, AnyStr, Optional, Match, Iterable, Any
from dataclasses import dataclass

@dataclass
class BlockCoordinate:
    x: int
    y: int
    z: int

@dataclass
class ChunkCoordinate:
    x: int
    y: int
    z: int

    def getMinBlock(self):
        return BlockCoordinate(
            x = self.x << 4,
            y = self.y << 4,
            z = self.z << 4,
        )

    def getMaxBlock(self):
        return BlockCoordinate(
            x = (self.x + 1 << 4) - 1,
            y = (self.y + 1 << 4) - 1,
            z = (self.z + 1 << 4) - 1,
        )

@dataclass
class RegionCoordinate:
    x: int
    z: int

    def getMinChunk(self):
        return ChunkCoordinate(
            x = self.x << 5,
            y = 0,
            z = self.z << 5,
        )

    def getMaxChunk(self):
        return ChunkCoordinate(
            x = (self.x + 1 << 5) - 1,
            y = 15,
            z = (self.z + 1 << 5) - 1,
        )

    def getMinBlock(self):
        coords = self.getMinChunk()
        return coords.getMinBlock()

    def getMaxBlock(self):
        coords = self.getMaxChunk()
        return coords.getMaxBlock()

class LogParser:
    def __init__(self, name: str, pattern: str, create_sql: str, insert_sql: str):
        self.name = name
        self.pattern = re.compile(pattern)
        self.create_sql = create_sql
        self.insert_sql = insert_sql

    def parse(self, line:AnyStr) -> Optional[Match[AnyStr]]:
        return self.pattern.match(line)

    def insert(self, con: sqlite3.Connection, parameters: Iterable[Any]):
        cur = con.cursor()
        return con.execute(self.insert_sql, parameters)

    def create(self, con: sqlite3.Connection):
        cur = con.cursor()
        return cur.execute(self.create_sql)

class WorldParser:
    ...

parsers = [
    LogParser(
        name='logged_in',
        pattern='(?P<player>.+)\[\/(?P<ip>\d+\.\d+.\d+.\d+):(?P<port>\d+)\] logged in with entity id (?P<entityid>\d+) at \((?P<x>-?\d+.\d+), (?P<y>-?\d+.\d+), (?P<z>-?\d+.\d+)\)',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_LOGGED_IN(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level, player, ip, port, entityid, x, y, z)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_LOGGED_IN VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    LogParser(
        name='joined_game',
        pattern='(?P<player>.+) joined the game',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_JOINED_GAME(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_JOINED_GAME VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    LogParser(
        name='left_game',
        pattern='(?P<player>.+) left the game',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_LEFT_GAME(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_LEFT_GAME VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    LogParser(
        name='lost_connection',
        pattern='(?P<player>.+) lost connection: Disconnected',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_LOST_CONNECTION(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_LOST_CONNECTION VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    LogParser(
        name='uuid_player',
        pattern='UUID of player (?P<player>.+) is (?P<uuid>[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12})',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_UUID_PLAYER(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level, player, uuid)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_UUID_PLAYER VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    LogParser(
        name='moved_quickly',
        pattern='(?P<player>.+) moved too quickly! (?P<x>-?\d+.\d+),(?P<y>-?\d+.\d+),(?P<z>-?\d+.\d+)',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_MOVED_TOO_QUICKLY(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level, player, x, y, z)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_MOVED_TOO_QUICKLY VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    LogParser(
        name="crash_report_saved",
        pattern="This crash report has been saved to:",
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_CRASH_REPORT_SAVED(source_id, file_id, log_path, line, end_line, log_datetime timestamp, level)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_CRASH_REPORT_SAVED VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
]

def write_source(database: Union[bytes, Text], input_path: pathlib.Path):
    run_date = datetime.datetime.now()

    with sqlite3.connect(database=database) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_SOURCE(run_date timestamp, input_path TEXT)")
        con.commit()
        cur.execute(
            "INSERT INTO MINECRAFT_SERVER_SOURCE VALUES(?, ?)", (
                run_date,
                str(input_path),
            )
        )
        con.commit()

        return cur.lastrowid

def calculate_hash(path: pathlib.Path, buffer_size: int = 32768):
    md5 = hashlib.md5()
    sha1 = hashlib.sha1()

    with path.open("rb") as fp:
        while True:
            data = fp.read(buffer_size)
            if not data:
                break
            md5.update(data)
            sha1.update(data)

    return (md5, sha1)

def write_file_info(database: Union[bytes, Text], path: pathlib.Path, source_id: int, buffer_size: int = 32768):
    md5, sha1 = calculate_hash(path=path, buffer_size=buffer_size)
    
    # TODO: Split time stats into FILE_INFO_JOIN_SOURCE_INFO
    stat = path.stat()

    with sqlite3.connect(database=database) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_FILE(source_id, path TEXT, st_atime REAL, st_ctime REAL, st_mtime REAL, st_size REAL, md5 TEXT, sha1 TEXT)")
        con.commit()
        cur.execute(
            "INSERT INTO MINECRAFT_SERVER_FILE VALUES(?, ?, ?, ?, ?, ?, ?, ?)", (
                source_id,
                str(path),
                stat.st_atime,
                stat.st_ctime, 
                stat.st_mtime,
                stat.st_size,
                md5.hexdigest(),
                sha1.hexdigest(),
            )
        )
        con.commit()

        return cur.lastrowid

def parse_log_line(con: sqlite3.Connection, log_path: pathlib.Path, line: str, source_id: int, file_id: int):
    if not line[0] == "[":
        return
    
    log_datetime = datetime.datetime.strptime(line[1:23], "%d%b%Y %H:%M:%S.%f")
    
    start = line.find('[', 24)
    end = line.find(']', start)
    level = line[start+1:end]

    start = line.find('[', end)
    end = line.find(']', start)
    name = line[start+1:end]
    end = line.find(':', end)
    end_line = line[end+2:]

    for parser in parsers:
        m = parser.parse(end_line)
        if not m:
            continue
        
        parameters = [
            source_id, # source_id
            file_id, # file_id
            str(log_path), # log_path
            line, # line
            end_line, # end_line
            log_datetime, # log_datetime
            level, # log_level
        ]

        for group in m.groups():
            parameters.append(group)

        parser.insert(
            con=con, 
            parameters=parameters,
        )

def parse_log(database: Union[bytes, Text], log_path: pathlib.Path, log_file: TextIOWrapper, source_id: int):
    file_id = write_file_info(database=database, path=log_path, source_id=source_id)
    
    with sqlite3.connect(database=database) as con:
        for line in log_file.readlines():
            data = parse_log_line(
                source_id = source_id,
                file_id = file_id,
                con = con, 
                log_path = log_path, 
                line= line
            )
            con.commit()

def parse_ops(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    path = input_path / 'ops.json'
    write_file_info(database=database, path=path, source_id=source_id)

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_OPS(source_id, uuid, name, level, bypassesPlayerLimit)")
        con.commit()

        print(f"Parsing {path}")
        with path.open("r") as fp:
            for player in json.load(fp):
                con.execute(
                    "INSERT INTO MINECRAFT_SERVER_OPS VALUES(?, ?, ?, ?, ?)", (
                        source_id,
                        player["uuid"],
                        player["name"],
                        player["level"],
                        player["bypassesPlayerLimit"],
                    ),
                )
        con.commit()

def parse_whitelist(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    path = input_path / 'whitelist.json'
    write_file_info(database=database, path=path, source_id=source_id)

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_WHITELIST(source_id, uuid, name)")
        con.commit()

        print(f"Parsing {path}")
        with path.open("r") as fp:
            for player in json.load(fp):
                con.execute(
                    "INSERT INTO MINECRAFT_SERVER_WHITELIST VALUES(?, ?, ?)", (
                        source_id,
                        player["uuid"],
                        player["name"],
                    ),
                )
        con.commit()

def parse_usercache(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    path = input_path / 'usercache.json'
    write_file_info(database=database, path=path, source_id=source_id)

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_USERCACHE(source_id, uuid, name, expiresOn)")
        con.commit()

        print(f"Parsing {path}")
        with path.open("r") as fp:
            for player in json.load(fp):
                con.execute(
                    "INSERT INTO MINECRAFT_SERVER_USERCACHE VALUES(?, ?, ?, ?)", (
                        source_id,
                        player["uuid"],
                        player["name"],
                        player["expiresOn"],
                    ),
                )
        con.commit()

regions_pattern = re.compile("r\.(?P<region_x>-?\d+)\.(?P<region_z>-?\d+)\.mca")

def parse_region(database: Union[bytes, Text], path: pathlib.Path, source_id: int):
    m = regions_pattern.match(path.name)
    if not m:
        return
    groups = m.groupdict()

    file_id = write_file_info(database=database, path=path, source_id=source_id)
    with sqlite3.connect(database=database) as con:
        region = RegionCoordinate(
            x = int(groups["region_x"]),
            z = int(groups["region_z"]),
        )

        min_block = region.getMinBlock() 
        max_block = region.getMaxBlock()

        con.execute(
            "INSERT INTO MINECRAFT_SERVER_REGIONS VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                source_id, # source_id
                file_id, # file_id
                str(path), # path
                region.x, # region_x
                region.z, # region_z
                min_block.x, # min_x 
                min_block.y, # min_y 
                min_block.z, # min_z
                max_block.x, # max_x 
                max_block.y, # max_y 
                max_block.z, # max_z
            ),
        )
    con.commit()

def parse_regions(database: Union[bytes, Text], world_path: pathlib.Path, source_id: int):
    region_path = world_path / 'region'

    print(f"Parsing {region_path}")
    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_REGIONS(source_id, file_id, path TEXT, region_x INTEGER, region_z INTEGER, min_x INTEGER, min_y INTEGER, min_z INTEGER, max_x INTEGER, max_y INTEGER, max_z INTEGER)")
        con.commit()

    for path in region_path.iterdir():
        parse_region(database=database, path=path, source_id=source_id)

def parse_stats(database: Union[bytes, Text], world_path: pathlib.Path, source_id: int):
    stats_path = world_path / 'stats'
    print(f"Parsing {stats_path}")
    for path in stats_path.iterdir():
        ...

def parse_server_properties(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    path = input_path / 'server.properties'
    file_id = write_file_info(database=database, path=path, source_id=source_id)

    level_name = 'world'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_SERVER_PROPERTIES(source_id, file_id, key, value)")
        con.commit()

        print(f"Parsing {path}")
        with path.open("r") as fp:
            for line in fp.readlines():
                if line.startswith("#"):
                    continue
                key, value = line.split("=", 1)
                if key == "level-name":
                    level_name = value.strip()
                con.execute(
                    "INSERT INTO MINECRAFT_SERVER_SERVER_PROPERTIES VALUES(?, ?, ?, ?)", (
                        source_id,
                        file_id,
                        key,
                        value,
                    )
                )
        con.commit()

    world_path = input_path / level_name

    for parse in [
        parse_regions
    ]:
        parse(database=database, world_path=world_path, source_id=source_id)

def parse_logs(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    with sqlite3.connect(database=database) as con:
        for parser in parsers:
            parser.create(con)
        con.commit()

    logs_path = input_path / 'logs'

    print(f"Parsing {logs_path}")
    for log_path in logs_path.iterdir():
        if log_path.name.endswith(".log"):
            with log_path.open('rt') as log_file:
                parse_log(database=database, log_path=log_path, log_file=log_file, source_id=source_id)
            continue

        if log_path.name.endswith(".log.gz"):            
            with gzip.open(log_path, 'rt') as log_file:
                parse_log(database=database, log_path=log_path, log_file=log_file, source_id=source_id)
            continue

def parse_crash_reports(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    crash_reports_path = input_path / 'crash-reports'
    print(f"Parsing {crash_reports_path}")

    #TODO: use LogParser???
    #TODO: player_name to player? for consistancy?
    line_pattern = re.compile("\tPlayer Count: (?P<current_player_count>[0-9]+) / (?P<max_player_count>[0-9]+); \[(?P<player_details>.+)\]")
    player_details_pattern = re.compile("ServerPlayer\[\'(?P<player_name>[^\']+)\'/(?P<entityid>\d+), l=\'(?P<level_name>[^\']+)\', x=(?P<x>[^,]+), y=(?P<y>[^,]+), z=(?P<z>[^,]+)\]")

    with sqlite3.connect(database=database) as con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_CRASH_REPORTS_PLAYER_DETAILS(source_id, file_id, path, log_datetime timestamp, line, player_name, entityid, level_name, x, y, z)"
        )
        con.commit()
        
        insert_sql="INSERT INTO MINECRAFT_SERVER_CRASH_REPORTS_PLAYER_DETAILS VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        for crash_report_path in crash_reports_path.iterdir():
            file_id = write_file_info(database=database, path=crash_report_path, source_id=source_id)

            # TODO: Use the parse thing used in logs
            log_datetime = datetime.datetime(
                int(crash_report_path.name[6:10]),
                int(crash_report_path.name[11:13]),
                int(crash_report_path.name[14:16]),
                int(crash_report_path.name[17:19]),
                int(crash_report_path.name[20:22]),
                int(crash_report_path.name[23:25]),
            )

            with crash_report_path.open("r") as file:
                for line in file.readlines():
                    match = line_pattern.match(line)
                    if match is None:
                        continue
                    player_count_details = match.groupdict()
                    players_details = player_count_details['player_details']

                    for player_details_match in player_details_pattern.finditer(players_details):
                        player_details = player_details_match.groupdict()
                        params = (
                            source_id, 
                            file_id, 
                            #TODO: Remove paths, file_id will do
                            str(crash_report_path), # path 
                            log_datetime, # log_datetime
                            line,        
                            player_details["player_name"], # player_name, 
                            player_details["entityid"], # entityid, 
                            player_details["level_name"], # level_name, 
                            player_details["x"], # x, 
                            player_details["y"], # y, 
                            player_details["z"],
                        )
                        con.execute(
                            insert_sql, 
                            params,
                        )

                    con.commit()

SESSION_SELECT_SQL = '''
SELECT
    source_id,
	rowid, 
	log_datetime,
	"MINECRAFT_SERVER_LOGS_LOGGED_IN" as table_type,
    "joined" as type,
	player
FROM
	MINECRAFT_SERVER_LOGS_LOGGED_IN
WHERE source_id = ?
UNION
SELECT
    source_id,
	rowid, 
	log_datetime,
	"MINECRAFT_SERVER_LOGS_LEFT_GAME" as table_type,
    "left" as type,
	player
FROM
	MINECRAFT_SERVER_LOGS_LEFT_GAME
WHERE source_id = ?
UNION
SELECT
    source_id,
	rowid, 
	log_datetime,
	"MINECRAFT_SERVER_CRASH_REPORTS_PLAYER_DETAILS" as table_type,
    "left" as type,
	player_name as player
FROM
	MINECRAFT_SERVER_CRASH_REPORTS_PLAYER_DETAILS
WHERE source_id = ?
ORDER BY player, log_datetime
'''

def parse_sessions(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    players = list()

    print(f"Parsing sessions")

    with sqlite3.connect(database=database, detect_types=sqlite3.PARSE_DECLTYPES) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_SESSIONS(source_id, player, left_id, login_id, left_time timestamp, login_time timestamp, left_type, login_type, duration)")
        con.commit()

        cur_player, login_id, login_time, login_type = None, None, None, None

        res = cur.execute(SESSION_SELECT_SQL, [source_id, source_id, source_id])
        for row in res.fetchall():
            source_id, rowid, log_datetime, table_type, event_type, player = row
            if event_type == "joined":
                cur_player, login_id, login_time, login_type = player, rowid, log_datetime, table_type
            elif event_type == "left":
                if player == cur_player:
                    duration: datetime.timedelta = log_datetime - login_time
                    params = (
                        source_id, # source_id
                        player, # player
                        rowid, # left_id
                        login_id, # login_id
                        log_datetime, # left_time
                        login_time, # login_time
                        table_type, # left_type
                        login_type, # login_type
                        duration.total_seconds(),
                    )
                    cur.execute("INSERT INTO MINECRAFT_SERVER_SESSIONS VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", params)
                    con.commit()
                cur_player, login_id, login_time, login_type = None, None, None, None

def create_parser():
    parser = argparse.ArgumentParser(
        prog='MinecraftLogParser',
    )

    parser.add_argument('-i', '--input', default="minecraftserver")
    parser.add_argument('-o', '--output', default="results.db")

    return parser

def main(args=None, namespace=None):
    parser = create_parser()
    args = parser.parse_args(args=args, namespace=namespace)

    database = args.output
    input_path = pathlib.Path(args.input)

    source_id = write_source(database=database, input_path=input_path)
    for parse in [
        parse_crash_reports,
        parse_ops, 
        parse_whitelist, 
        parse_usercache, 
        parse_server_properties, 
        parse_logs,
        parse_sessions,
    ]:
        parse(database=database, input_path=input_path, source_id=source_id)

if __name__ == "__main__":
    main()