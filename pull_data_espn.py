import itertools
import models
import re
import requests
from bs4 import BeautifulSoup

def get(url):
    full_url = "http://games.espn.com" + url
    cookies = {
    }
    obj = requests.get(full_url, cookies=cookies)
    return obj.text

def get_links_for_week(week):
    page = get("/ffl/scoreboard?leagueId=754986&matchupPeriodId=" + str(week))

    games = []
    bs = BeautifulSoup(page)
    for link in bs.find_all("a", string="Full Box Score"):
        games.append(link.get("href"))
    return games

def get_game_stats(week, game_link):
    page = get(game_link)
    bs = BeautifulSoup(page)
    team_infos = bs.select("#teamInfos")[0]

    def extract_team_id(href):
        result = re.match(".*&teamId=(.*)$", href)
        return models.Team.ID_TO_OWNER.get(int(result.group(1)))

    team1 = extract_team_id(team_infos.div.div.div.a.get('href'))
    team2 = extract_team_id(team_infos.div.next_sibling.div.div.a.get('href'))

    game_id = str(week) + "_" + team1 + "_" + team2
    team_id = team1

    def get_table_type(table):
        if len(tables) == 0:
            return None
        table = tables[0]
        row = table.tr
        if row.th is None:
            row = row.next_sibling
        if row.th is None:
            return None
        return row.th.string

    def read_column(columns):
        column = read_column_raw(columns)
        print column
        if column is None:
            return "0"
        val = column.string
        if val is None:
            return "0"
        return val.replace("--", "0")

    def read_column_raw(columns):
        if len(columns) == 0:
            return None
        column = columns.pop(0)
        return column

    def get_player_id(player_info):
        return player_info.a.get('playerid')

    def get_player_name(player_info):
        return player_info.a.string

    def get_player_position(player_info):
        return player_info.a.next_sibling.string.split()[-1]

    def get_offense_stats(tables):
        all_data = []
        if len(tables) == 0:
            return all_data
        table = tables[0]
        if "OFFENSIVE PLAYERS" not in get_table_type(table):
            return all_data
        tables.pop(0)

        for row in table.select(".pncPlayerRow"):
            data = {
                "game_id": game_id,
                "team_id": team_id,
            }
            columns = row.select("td")
            data["slot"] = read_column(columns)
            player_info = read_column_raw(columns)
            if player_info.a is None:
                # freaking homan not playing a defense
                continue
            data["player_id"] = get_player_id(player_info)
            data["player_name"] = get_player_name(player_info)
            data["player_pos"] = get_player_position(player_info)

            maybe_bye_week = read_column(columns)
            if "BYE" not in maybe_bye_week:
                # skip past
                read_column(columns)

            read_column(columns)
            atts_comps = read_column(columns).split("/")
            print atts_comps, game_id
            data["pass_comps"] = int(atts_comps[0])
            data["pass_atts"] = int(atts_comps[1])
            data["pass_yards"] = int(read_column(columns))
            data["pass_tds"] = int(read_column(columns))
            data["pass_ints"] = int(read_column(columns))
            read_column(columns)
            data["rush_attempts"] = int(read_column(columns))
            data["rush_yards"] = int(read_column(columns))
            data["rush_tds"] = int(read_column(columns))
            read_column(columns)
            data["receptions"] = int(read_column(columns))
            data["rec_yards"] = int(read_column(columns))
            data["rec_tds"] = int(read_column(columns))
            data["rec_targets"] = int(read_column(columns))
            read_column(columns)
            data["two_pt"] = int(read_column(columns))
            data["fumbles_lost"] = int(read_column(columns))
            data["misc_td"] = int(read_column(columns))
            read_column(columns)
            data["total_points"] = int(read_column(columns))
            all_data.append(models.PlayerGameStats(**data))

        return all_data

    def get_kicker_stats(tables):
        all_data = []
        if len(tables) == 0:
            return all_data
        table = tables[0]
        if "KICKERS" not in get_table_type(table):
            return all_data
        tables.pop(0)
        for row in table.select(".pncPlayerRow"):
            data = {
                "game_id": game_id,
                "team_id": team_id,
            }
            columns = row.select("td")
            data["slot"] = read_column(columns)
            player_info = read_column_raw(columns)
            if player_info.a is None:
                # freaking homan not playing a defense
                continue
            data["player_id"] = get_player_id(player_info)
            data["player_name"] = get_player_name(player_info)
            data["player_pos"] = get_player_position(player_info)

            maybe_bye_week = read_column(columns)
            if "BYE" not in maybe_bye_week:
                # skip past
                read_column(columns)

            read_column(columns)
            fg_1_39 = read_column(columns).split("/")
            data["fgm_1_39"] = int(fg_1_39[0])
            data["fga_1_39"] = int(fg_1_39[1])
            fg_40_49 = read_column(columns).split("/")
            data["fgm_40_49"] = int(fg_40_49[0])
            data["fga_40_49"] = int(fg_40_49[1])
            fg_50 = read_column(columns).split("/")
            data["fgm_50"] = int(fg_50[0])
            data["fga_50"] = int(fg_50[1])
            fg = read_column(columns).split("/")
            data["fgm"] = int(fg[0])
            data["fga"] = int(fg[1])
            fg_xp = read_column(columns).split("/")
            data["fgm_xp"] = int(fg_xp[0])
            data["fga_xp"] = int(fg_xp[1])
            read_column(columns)
            data["total_points"] = int(read_column(columns))
            all_data.append(models.PlayerGameStats(**data))
        return all_data

    def get_defense_stats(tables):
        all_data = []
        if len(tables) == 0:
            return all_data
        table = tables[0]
        if "TEAM D" not in get_table_type(table):
            return all_data
        tables.pop(0)
        for row in table.select(".pncPlayerRow"):
            data = {
                "game_id": game_id,
                "team_id": team_id,
            }
            columns = row.select("td")
            data["slot"] = read_column(columns)
            player_info = read_column_raw(columns)
            if player_info.a is None:
                # freaking homan not playing a defense
                continue
            data["player_id"] = get_player_id(player_info)
            data["player_name"] = get_player_name(player_info)
            data["player_pos"] = get_player_position(player_info)

            maybe_bye_week = read_column(columns)
            if "BYE" not in maybe_bye_week:
                # skip past
                read_column(columns)
            read_column(columns)

            data["def_tds"] = int(read_column(columns))
            data["def_ints"] = int(read_column(columns))
            data["def_fumbles_rec"] = int(read_column(columns))
            data["def_sacks"] = int(read_column(columns))
            data["def_safeties"] = int(read_column(columns))
            data["def_blocks"] = int(read_column(columns))
            data["def_points_allowed"] = int(read_column(columns))
            read_column(columns)
            data["total_points"] = int(read_column(columns))
            all_data.append(models.PlayerGameStats(**data))
        return all_data


    tables = bs.select(".playerTableTable")
    stats = itertools.chain(
        get_offense_stats(tables),
        get_kicker_stats(tables),
        get_defense_stats(tables),
        get_offense_stats(tables),
        get_kicker_stats(tables),
        get_defense_stats(tables),
    )

    team_id = team2 # kind of a hack to set the correct time id

    stats = itertools.chain(
        stats,
        get_offense_stats(tables),
        get_kicker_stats(tables),
        get_defense_stats(tables),
        get_offense_stats(tables),
        get_kicker_stats(tables),
        get_defense_stats(tables),
    )

    return list(stats)

with open("data/00header.csv", "w") as fp:
    fp.write(models.PlayerGameStats.getHeaderCSVRow())
    fp.write("\n")

for week in range(4, 17):
    stats_for_week = []
    for game in get_links_for_week(week):
        stats_for_week.extend(get_game_stats(week, game))

    # write stats to file
    with open("data/{:0>2d}.csv".format(week), "w") as fp:
        for stats in stats_for_week:
            stats.serialize(fp)
