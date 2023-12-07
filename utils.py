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
        create_sql="CREATE TABLE IF NOT EXISTS LOGS_LOGGED_IN(run_date timestamp, log_path, line, end_line, log_datetime timestamp, level, player, ip, port, entityid, x, y, z)",
        insert_sql="INSERT INTO LOGS_LOGGED_IN VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='joined_game',
        pattern='(?P<player>.+) joined the game',
        create_sql="CREATE TABLE IF NOT EXISTS LOGS_JOINED_GAME(run_date timestamp, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO LOGS_JOINED_GAME VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='left_game',
        pattern='(?P<player>.+) left the game',
        create_sql="CREATE TABLE IF NOT EXISTS LOGS_LEFT_GAME(run_date timestamp, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO LOGS_LEFT_GAME VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='lost_connection',
        pattern='(?P<player>.+) lost connection: Disconnected',
        create_sql="CREATE TABLE IF NOT EXISTS LOGS_LOST_CONNECTION(run_date timestamp, log_path, line, end_line, log_datetime timestamp, level, player)",
        insert_sql="INSERT INTO LOGS_LOST_CONNECTION VALUES(?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='uuid_player',
        pattern='UUID of player (?P<player>.+) is (?P<uuid>[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12})',
        create_sql="CREATE TABLE IF NOT EXISTS LOGS_UUID_PLAYER(run_date timestamp, log_path, line, end_line, log_datetime timestamp, level, player, uuid)",
        insert_sql="INSERT INTO LOGS_UUID_PLAYER VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
    ),
    Parser(
        name='moved_quickly',
        pattern='(?P<player>.+) moved too quickly! (?P<x>-?\d+.\d+),(?P<y>-?\d+.\d+),(?P<z>-?\d+.\d+)',
        create_sql="CREATE TABLE IF NOT EXISTS LOGS_MOVED_TOO_QUICKLY(run_date timestamp, log_path, line, end_line, log_datetime timestamp, level, player, x, y, z)",
        insert_sql="INSERT INTO LOGS_MOVED_TOO_QUICKLY VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
]

def read_line(con: sqlite3.Connection, log_path: pathlib.Path, line: str, run_date: datetime.datetime):
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
            run_date, # run_date
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

def create_parser():
    parser = argparse.ArgumentParser(
        prog='MinecraftLogParser',
    )

    parser.add_argument('-i', '--input', default=".")
    parser.add_argument('-o', '--output', default="results.db")

    return parser

def parse(database: Union[bytes, Text], log_path: pathlib.Path, log_file: TextIOWrapper, run_date: datetime.datetime):
    with sqlite3.connect(database=database) as con:
        for line in log_file.readlines():
            data = read_line(
                run_date = run_date,
                con = con, 
                log_path = log_path, 
                line= line
            )
            con.commit()

def parse_ops(database: Union[bytes, Text], input_path: pathlib.Path, run_date: datetime.datetime):
    path = input_path / 'ops.json'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS OPS(run_date timestamp, uuid, name, level, bypassesPlayerLimit)")
        con.commit()

        with path.open("r") as fp:
            for player in json.load(fp):
                con.execute(
                    "INSERT INTO OPS VALUES(?, ?, ?, ?, ?)", (
                        run_date,
                        player["uuid"],
                        player["name"],
                        player["level"],
                        player["bypassesPlayerLimit"],
                    ),
                )
        con.commit()

def parse_whitelist(database: Union[bytes, Text], input_path: pathlib.Path, run_date: datetime.datetime):
    path = input_path / 'whitelist.json'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS WHITELIST(run_date timestamp, uuid, name)")
        con.commit()

        with path.open("r") as fp:
            for player in json.load(fp):
                con.execute(
                    "INSERT INTO WHITELIST VALUES(?, ?, ?)", (
                        run_date,
                        player["uuid"],
                        player["name"],
                    ),
                )
        con.commit()

def parse_usercache(database: Union[bytes, Text], input_path: pathlib.Path, run_date: datetime.datetime):
    path = input_path / 'usercache.json'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS USERCACHE(run_date timestamp, uuid, name, expiresOn)")
        con.commit()

        with path.open("r") as fp:
            for player in json.load(fp):
                con.execute(
                    "INSERT INTO USERCACHE VALUES(?, ?, ?, ?)", (
                        run_date,
                        player["uuid"],
                        player["name"],
                        player["expiresOn"],
                    ),
                )
        con.commit()

regions_pattern = re.compile("r\.(?P<region_x>-?\d+)\.(?P<region_z>-?\d+)\.mca")

def parse_regions(database: Union[bytes, Text], world_path: pathlib.Path, run_date: datetime.datetime):
    region_path = world_path / 'region'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_REGIONS(run_date timestamp, path TEXT, region_x INTEGER, region_z INTEGER, min_x INTEGER, min_y INTEGER, min_z INTEGER, max_x INTEGER, max_y INTEGER, max_z INTEGER)")
        con.commit()

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
                    run_date, # run_date
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

def parse_server_properties(database: Union[bytes, Text], input_path: pathlib.Path, run_date: datetime.datetime):
    path = input_path / 'server.properties'

    level_name = 'world'

    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS SERVER_PROPERTIES(run_date timestamp, key, value)")
        con.commit()

        with path.open("r") as fp:
            for line in fp.readlines():
                if line.startswith("#"):
                    continue
                key, value = line.split("=", 1)
                if key == "level-name":
                    level_name = value.strip()
                con.execute(
                    "INSERT INTO SERVER_PROPERTIES VALUES(?, ?, ?)", (
                        run_date,
                        key,
                        value,
                    )
                )
        con.commit()

    world_path = input_path / level_name
    parse_regions(database=database, world_path=world_path, run_date=run_date)

def write_run(database: Union[bytes, Text], input_path: pathlib.Path, run_date: datetime.datetime):
    with sqlite3.connect(database=database) as con:
        con.execute("CREATE TABLE IF NOT EXISTS MINECRAFT_SERVER_RUN(run_date timestamp)")
        con.commit()
        con.execute(
            "INSERT INTO MINECRAFT_SERVER_RUN VALUES(?)", (
                run_date,
            )
        )

        con.commit()

def parse_logs(database: Union[bytes, Text], input_path: pathlib.Path, run_date: datetime.datetime):
    with sqlite3.connect(database=database) as con:
        for parser in parsers:
            parser.create(con)
        con.commit()

    logs_path = input_path / 'logs'
    for log_path in logs_path.iterdir():
        if log_path.name.endswith(".log"):
            with log_path.open('rt') as log_file:
                parse(database=database, log_path=log_path, log_file=log_file, run_date=run_date)
            continue

        if log_path.name.endswith(".log.gz"):            
            with gzip.open(log_path, 'rt') as log_file:
                parse(database=database, log_path=log_path, log_file=log_file, run_date=run_date)
            continue

def main(args=None, namespace=None):
    parser = create_parser()
    args = parser.parse_args(args=args, namespace=namespace)

    database = args.output
    input_path = pathlib.Path(args.input)

    run_date = datetime.datetime.now()

    write_run(database=database, input_path=input_path, run_date=run_date)
    parse_ops(database=database, input_path=input_path, run_date=run_date)
    parse_whitelist(database=database, input_path=input_path, run_date=run_date)
    parse_usercache(database=database, input_path=input_path, run_date=run_date)
    parse_server_properties(database=database, input_path=input_path, run_date=run_date)
    parse_logs(database=database, input_path=input_path, run_date=run_date)

if __name__ == "__main__":
    main()