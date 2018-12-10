import os
import sys
sys.path.append("../modules/")
import databasefunctions as dbf

REPLAYDIR = "C:/Users/Aili/Documents/Heroes of the Storm/Accounts/403236886/2-Hero-1-2412655/Replays/Multiplayer/"

files = os.listdir(REPLAYDIR)
files = map(lambda x: REPLAYDIR + x, files)

dbf.create_database("../herogames.db", 
                    [dbf.create_matches_table, 
                     dbf.create_player_stats_table, 
                     dbf.create_experience_table, 
                     dbf.create_deaths_table, 
                     dbf.create_structure_deaths_table])

with dbf.create_connection("../herogames.db") as conn:
    failed_files = dbf.log_replays(conn, files)
    conn.commit()