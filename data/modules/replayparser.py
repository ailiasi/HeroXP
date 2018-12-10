from heroprotocol.mpyq import mpyq
#An initial protocol to read the header
from heroprotocol import protocol29406
import importlib
import pandas as pd
#import hashlib
import uuid


def loops_to_seconds(loops):
    #Games start at loop 610 and run at 16 loops per second
    return (loops - 610) / 16.0


def fixed_data_to_real(value):
    return value/4096.0


def win_filetime_to_unix(filetime):
    return (filetime/10000000) - 11644473600


def read_details(details):
    matchTimeLocal = win_filetime_to_unix(details['m_timeUTC'])
    timeOffsetUTC = details['m_timeLocalOffset']/10000000
    matchTimeUTC = matchTimeLocal - timeOffsetUTC #Check that this works for times after UTC
    map_title = details['m_title']
    
    players = []
    for player in details['m_playerList']:
        if player['m_observe'] == 0:
            name = player['m_name']
            battleNetId = player['m_toon']['m_id']
            toonHandle = str(player['m_toon']['m_region']) + '-' + str(player['m_toon']['m_programId']) + '-' + str(player['m_toon']['m_realm']) + '-' + str(player['m_toon']['m_id'])
            hero = player['m_hero']
            #team = player['m_teamId']
            result = player['m_result']
            players.append({'Name': name, 'ToonHandle': toonHandle, 'Hero': hero, 'Result': result, "BattleNetId": battleNetId})
    return matchTimeLocal, matchTimeUTC, map_title, players


def read_tracker_events(tracker_events):
    player_inits = []
    xp_stats = {"GameTime": [],
                "Team": [],
                "PreviousGameTime": [],
                "MinionXP": [],
                "CreepXP": [],
                "StructureXP": [],
                "HeroXP": [],
                "TrickleXP": [],
               }
    deaths = []
    structure_deaths = []
    towers = pd.DataFrame()
    
    for event in tracker_events:
        #TODO: Track structure deaths
        #TODO: Track objectives
        if "m_eventName" in event:
            if event['m_eventName'] == 'PlayerInit':
                if event["m_stringData"][0]['m_value'] == "User":
                    toonHandle = event['m_stringData'][1]['m_value']
                else:
                    toonHandle = "0-\x00\x00\x00\x00-0-0"
                player_inits.append({"PlayerId": event['m_intData'][0]['m_value'], 
                                     "ToonHandle": toonHandle, 
                                     "Team": event['m_intData'][1]['m_value']})
            
            if event["m_eventName"] == 'PeriodicXPBreakdown':
                xp_stats['Team'].append(event["m_intData"][0]["m_value"])
                for stat in event["m_fixedData"]:
                    xp_stats[stat['m_key']].append(fixed_data_to_real(stat['m_value']))
                    
            #TODO: add end of game breakdown        
            if event["m_eventName"] == "EndOfGameXPBreakdown":
                pass
            
            if event['m_eventName'] == 'PlayerDeath':
                #TODO: change to actual time
                deaths.append({"PlayerId": event["m_intData"][0]["m_value"], 
                               "Time": loops_to_seconds(event["_gameloop"])})
        
        if event["_eventid"] == 1 and event["m_unitTypeName"].startswith('TownCannonTower'):
            towers = towers.append(pd.DataFrame([event]))

            
        if event["_eventid"] == 4 and event['m_unitTypeName'] == 'TownCannonTowerDead':
            towers_ind = towers.set_index(["m_unitTagIndex", "m_unitTagRecycle"])
            owner = towers_ind.loc[event["m_unitTagIndex"],event["m_unitTagRecycle"]]["m_upkeepPlayerId"] - 10
            
            structure_deaths.append({"Owner": owner,
                                     "Time": loops_to_seconds(event["_gameloop"]),
                                     "Type": "TownCannonTower"})
        
        if event["_eventid"] == 1 and event["m_unitTypeName"] == "TownHallDestroyed": #SUnitBornEvent
            structure_deaths.append({"Owner": event['m_upkeepPlayerId'] - 10,
                                     "Time": loops_to_seconds(event["_gameloop"]),
                                     "Type": "TownHall"})

    return player_inits, xp_stats, deaths, pd.DataFrame(structure_deaths)


def read_initdata(initdata):
    modes = {50021: "Versus AI",
             50041: "Practice",
             50001: "Quick Match",
             50031: "Brawl",
             50051: "Unranked Draft",
             50061: "Hero League",
             50071: "Team League",
             -1: "Custom",
             }
    mode = modes[initdata['m_syncLobbyState']['m_gameDescription']['m_gameOptions']['m_ammId']]
    random_seed = initdata['m_syncLobbyState']['m_gameDescription']['m_randomValue']
    return mode, random_seed


def read_replay(replay_file):
    #TODO: player match stats - df with end of match statistics for each player for each match
    archive = mpyq.MPQArchive(replay_file)

    # Read the protocol header, this can be read with any protocol
    contents = archive.header['user_data_header']['content']
    header = protocol29406.decode_replay_header(contents)

    baseBuild = header['m_version']['m_baseBuild']
    if baseBuild < 43905:
        raise NotImplementedError("File too old, build: {}".format(baseBuild))
        
    try:
        protocol = importlib.import_module('heroprotocol.protocol%s' % (baseBuild,))
        #protocol = __import__('heroprotocol.protocol%s' % (baseBuild,))
    except ImportError:
        print 'Unsupported base build: %d' % baseBuild
        
    #initdata
    contents = archive.read_file('replay.initData')
    initdata = protocol.decode_replay_initdata(contents)
    game_mode, random_seed = read_initdata(initdata)
    
    assert game_mode != "Versus AI" and game_mode != "Brawl", "AI matches and brawls not supported"        
        
    #details
    contents = archive.read_file('replay.details')
    details = protocol.decode_replay_details(contents)
    matchTimeLocal, matchTimeUTC, map_title, players = read_details(details)
    
    #tracker events
    contents = archive.read_file('replay.tracker.events')
    tracker_events = protocol.decode_replay_tracker_events(contents)
    player_inits, xp_stats, deaths, structure_deaths = read_tracker_events(tracker_events)
    
    players = pd.merge(pd.DataFrame(players), pd.DataFrame(player_inits), on = "ToonHandle")
    matchId = "".join(map(str, sorted(players["BattleNetId"]))) + str(random_seed)
    #TODO: Make sure that this is hotsapi compatible
    matchId = str(uuid.uuid3(uuid.NAMESPACE_OID, matchId))#hashlib.md5(matchId.encode("utf-8")))
    players.drop(columns = "BattleNetId", inplace = True)
    
    #TODO: find a better way to do this
    winner = players[players["Result"] == 1]["Team"].iloc[0]    
    match = {"MatchId": matchId, "LocalTime": matchTimeLocal, "UTCTime": matchTimeUTC, "Map": map_title, "Mode": game_mode, "Winner": winner}    
    xp_stats = pd.DataFrame(xp_stats)    
    deaths = pd.DataFrame(deaths)
    
    return players, match, xp_stats, deaths, structure_deaths