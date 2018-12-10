import replayparser
import sqlite3
from sqlite3 import Error


create_matches_table = """ CREATE TABLE IF NOT EXISTS matches (
                                id integer PRIMARY KEY,
                                match_id text,
                                time_local integer NOT NULL,
                                time_UTC integer,
                                map text NOT NULL,
                                mode text NOT NULL,
                                winner integer NOT NULL
                           ); """
    
#TODO: Think this through
create_player_stats_table = """ CREATE TABLE IF NOT EXISTS player_stats (
                                    id integer PRIMARY KEY,
                                    match_id integer NOT NULL,
                                    toon_handle text NOT NULL,
                                    name text NOT NULL,
                                    player_id integer NOT NULL,
                                    hero text NOT NULL,
                                    team integet NOT NULL,
                                    result integer NOT NULL
                                ); """
    
create_experience_table = """ CREATE TABLE IF NOT EXISTS experience (
                                id integer PRIMARY KEY,
                                match_id integer NOT NULL,
                                game_time real NOT NULL,
                                team integer NOT NULL,
                                minion_xp real,
                                hero_xp real,
                                structure_xp real,
                                creep_xp real,
                                tricle_xp real
                            ); """
    
create_deaths_table = """ CREATE TABLE IF NOT EXISTS deaths (
                                id integer PRIMARY KEY,
                                match_id integer NOT NULL,
                                time real,
                                player_id integer
                           ); """

create_structure_deaths_table = """ CREATE TABLE IF NOT EXISTS structure_deaths (
                                        id integer PRIMARY KEY,
                                        match_id integer NOT NULL,
                                        time real,
                                        owner integer,
                                        type text
                                    ); """


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        conn.text_factory = str
        return conn
    except Error as e:
        print(e)
 
    return None


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_database(database, create_tables_array):
    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        for create_table_sql in create_tables_array:
            create_table(conn, create_table_sql)
        conn.close()
    else:
        print("Error! cannot create the database connection.")

    
def create_match(conn, match): 
    sql = ''' INSERT INTO matches(match_id, time_local, time_UTC, map, mode, winner)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, match)
    return cur.lastrowid


def create_players(conn, players): 
    sql = ''' INSERT INTO player_stats(match_id, toon_handle, name, player_id, hero, team, result)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, players)
    return cur.lastrowid


def create_xp_stats(conn, xp_stats): 
    sql = ''' INSERT INTO experience(match_id, game_time, team, minion_xp, hero_xp, structure_xp, creep_xp, tricle_xp)
              VALUES(?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, xp_stats)
    return cur.lastrowid


def create_deaths(conn, deaths):
    sql = ''' INSERT INTO deaths(match_id, time, player_id)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, deaths)
    return cur.lastrowid

def create_structure_deaths(conn, structure_deaths):
    sql = ''' INSERT INTO structure_deaths(match_id, time, owner, type)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, structure_deaths)
    return cur.lastrowid


def log_replays(conn, replays):
    failed_files = []
    n = len(replays)
    for i, replay in enumerate(replays):
        try:
            players, match, xp_stats, deaths, structure_deaths = replayparser.read_replay(replay)
        except AssertionError as e:
            print e.message
            continue
        except NotImplementedError as e:
            print e.message
            continue
        except Exception as e:
            with open("failed_files.txt", "a") as f:
                f.write("Exception: {} on file {}\n".format(e.message, replay))
            failed_files.append(replay)
            print e
            #print "Exception: {} on file {}".format(e.message, replay)
            continue

        match_id = create_match(conn, [match['MatchId'], match['LocalTime'], match['UTCTime'], match['Map'], match['Mode'], int(match['Winner'])])#[match[item] for item in ['MatchId','LocalTime', 'UTCTime', 'Map', 'Mode', 'Winner']])

        players["MatchId"] = match_id
        xp_stats["MatchId"] = match_id
        deaths["MatchId"] = match_id
        structure_deaths["MatchId"] = match_id
        
        
        create_players(conn, 
                       players[["MatchId",
                                "ToonHandle",
                                "Name",
                                "PlayerId",
                                "Hero",
                                "Team",
                                "Result"]].values.tolist())
        create_xp_stats(conn, 
                        xp_stats[["MatchId", 
                                  "GameTime", 
                                  "Team", 
                                  "MinionXP", 
                                  "HeroXP", 
                                  "StructureXP", 
                                  "CreepXP", 
                                  "TrickleXP"]].values.tolist())
        create_deaths(conn, 
                      deaths[["MatchId",
                              "Time",
                              "PlayerId"]].values.tolist())
    
        create_structure_deaths(conn,
                                structure_deaths[["MatchId",
                                                  "Time",
                                                  "Owner",
                                                  "Type"]].values.tolist())
        if i%100 == 0:
            print float(i)/n
    
    return failed_files
        
        
        
        
        