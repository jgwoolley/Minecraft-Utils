'''
This is where the module documentation goes 
'''

import pathlib, gzip, datetime, json, re, argparse, sqlite3
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

class Parser:
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

parsers = [
    Parser(
        name='logged_in',
        pattern='(?P<player>.+)\[\/(?P<ip>\d+\.\d+.\d+.\d+):(?P<port>\d+)\] logged in with entity id (?P<entityid>\d+) at \((?P<x>-?\d+.\d+), (?P<y>-?\d+.\d+), (?P<z>-?\d+.\d+)\)',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_LOGGED_IN(source_id, log_path, line, end_line, log_datetime timestamp, level, player, ip, port, entityid, x, y, z)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_LOGGED_IN VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='joined_game',
        pattern='(?P<player>.+) joined the game',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_JOINED_GAME(source_id, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_JOINED_GAME VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='left_game',
        pattern='(?P<player>.+) left the game',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_LEFT_GAME(source_id, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_LEFT_GAME VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='lost_connection',
        pattern='(?P<player>.+) lost connection: Disconnected',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_LOST_CONNECTION(source_id, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_LOST_CONNECTION VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='uuid_player',
        pattern='UUID of player (?P<player>.+) is (?P<uuid>[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12})',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_UUID_PLAYER(source_id, log_path, line, end_line, log_datetime timestamp, level, player, uuid)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_UUID_PLAYER VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='moved_quickly',
        pattern='(?P<player>.+) moved too quickly! (?P<x>-?\d+.\d+),(?P<y>-?\d+.\d+),(?P<z>-?\d+.\d+)',
        create_sql="CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_LOGS_MOVED_TOO_QUICKLY(source_id, log_path, line, end_line, log_datetime timestamp, level, player, x, y, z)",
        insert_sql="INSERT INTO MINECRAFT_SERVER_LOGS_MOVED_TOO_QUICKLY VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
]

def read_line(con: sqlite3.Connection, log_path: pathlib.Path, line: str, source_id: int):
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

def parse(database: Union[bytes, Text], log_path: pathlib.Path, log_file: TextIOWrapper, source_id: int):
    with sqlite3.connect(database=database) as con:
        for line in log_file.readlines():
            data = read_line(
                source_id = source_id,
                con = con, 
                log_path = log_path, 
                line= line
            )
            con.commit()

def parse_ops(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    path = input_path / 'ops.json'

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

def parse_regions(database: Union[bytes, Text], world_path: pathlib.Path, source_id: int):
    region_path = world_path / 'region'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_REGIONS(source_id, path TEXT, region_x INTEGER, region_z INTEGER, min_x INTEGER, min_y INTEGER, min_z INTEGER, max_x INTEGER, max_y INTEGER, max_z INTEGER)")
        con.commit()

        print(f"Parsing {region_path}")
        for path in region_path.iterdir():
            m = regions_pattern.match(path.name)
            if not m:
                continue
            groups = m.groupdict()

            region = RegionCoordinate(
                x = int(groups["region_x"]),
                z = int(groups["region_z"]),
            )

            min_block = region.getMinBlock() 
            max_block = region.getMaxBlock()

            con.execute(
                "INSERT INTO MINECRAFT_SERVER_REGIONS VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    source_id, # source_id
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

def parse_server_properties(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    path = input_path / 'server.properties'

    level_name = 'world'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_SERVER_PROPERTIES(source_id, key, value)")
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
                    "INSERT INTO MINECRAFT_SERVER_SERVER_PROPERTIES VALUES(?, ?, ?)", (
                        source_id,
                        key,
                        value,
                    )
                )
        con.commit()

    world_path = input_path / level_name
    parse_regions(database=database, world_path=world_path, source_id=source_id)

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
                parse(database=database, log_path=log_path, log_file=log_file, source_id=source_id)
            continue

        if log_path.name.endswith(".log.gz"):            
            with gzip.open(log_path, 'rt') as log_file:
                parse(database=database, log_path=log_path, log_file=log_file, source_id=source_id)
            continue

def parse_sessions(database: Union[bytes, Text], input_path: pathlib.Path, source_id: int):
    players = list()

    print(f"Parsing sessions")

    with sqlite3.connect(database=database) as con:
        cur = con.cursor()
        res = cur.execute("SELECT DISTINCT player FROM MINECRAFT_SERVER_LOGS_LOGGED_IN")
        for row in res.fetchall():
            players.append(row[0])
    
    for player in players:
        login_times = list()
        with sqlite3.connect(database=database, detect_types=sqlite3.PARSE_DECLTYPES) as con:
            cur = con.cursor()
            res = cur.execute("SELECT log_datetime, rowid FROM MINECRAFT_SERVER_LOGS_LOGGED_IN WHERE player = ? AND source_id = ? ORDER BY log_datetime", [player, source_id])
            for row in res.fetchall():
                login_times.append((row[0], row[1]))
            cur.close()

        left_times = list()
        with sqlite3.connect(database=database, detect_types=sqlite3.PARSE_DECLTYPES) as con:
            cur = con.cursor()
            res = cur.execute("SELECT log_datetime, rowid FROM MINECRAFT_SERVER_LOGS_LEFT_GAME WHERE player = ? AND source_id = ? ORDER BY log_datetime", (player, source_id))
            for row in res.fetchall():
                left_times.append((row[0], row[1]))
            cur.close()

        with sqlite3.connect(database=database) as con:
            con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_SESSION(source_id, player, left_id, login_id, left_time timestamp, login_time timestamp, duration)")
            con.commit()
            print(f"For {player}, found {len(login_times)} login time(s), {len(left_times)} left time(s)")
            for (login_time, login_id), (left_time, left_id) in zip(login_times, left_times):
                duration = left_time - login_time
                con.execute(
                    "INSERT INTO MINECRAFT_SERVER_SESSION VALUES(?, ?, ?, ?, ?, ?, ?)", (
                        source_id, 
                        player, 
                        left_id,
                        login_id,
                        left_time, 
                        login_time, 
                        str(duration),
                    )
                )
                con.commit()

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
        parse_ops, 
        parse_whitelist, 
        parse_usercache, 
        parse_server_properties, 
        parse_logs,
        # parse_sessions,
    ]:
        parse(database=database, input_path=input_path, source_id=source_id)

if __name__ == "__main__":
    main()