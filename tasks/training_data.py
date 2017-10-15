from core.tasks import pandas_task

import nfldb
import pandas as pd

@pandas_task('training_wr.csv')
def training_wr(period):
    db = nfldb.connect()

    current_period = period
    data = {}
    q = (nfldb.Query(db)
        .game(season_year=current_period.season_year,
            week=current_period.week, season_type='Regular')
        .player(position='WR')
        .as_games())

    for game in q:
        for p in game.players:
            team, player = p
            if player.position != nfldb.Enums.player_pos.WR:
                continue
            player_tuple = (player.player_id, player.full_name)
            if not data.has_key(player_tuple):
                data[player_tuple] = {}
            data[player_tuple].update({
                'day_of_week': game.day_of_week,
                'def': game.home_team \
                    if game.home_team != team \
                    else game.away_team,
                'home': 1.0 \
                    if game.home_team == team \
                    else 0.0,
            })

    for i in range(5):
        q = (nfldb.Query(db)
            .game(season_year=current_period.season_year,
                week=current_period.week, season_type='Regular')
            .player(position='WR')
            .as_aggregate())

        if i == 0:
            suffix = ''
        else:
            suffix = '_' + str(i+1)

        for play_player in q:
            player = play_player.player
            player_tuple = (player.player_id, player.full_name)
            if not data.has_key(player_tuple):
                data[player_tuple] = {}
            data[player_tuple].update({
                'receiving_tar'+suffix: play_player.receiving_tar,
                'receiving_yds'+suffix: play_player.receiving_yds,
                'receiving_rec'+suffix: play_player.receiving_rec,
                'receiving_tds'+suffix: play_player.receiving_tds,
                'rushing_yds'+suffix: play_player.rushing_yds,
                'rushing_tds'+suffix: play_player.rushing_tds,
                'rushing_att'+suffix: play_player.rushing_att,
            })

        current_period = current_period.offset(-1)

    formatted = {}
    for player_tuple, stats in data.items():
        for stat, value in stats.items():
            if not formatted.has_key(stat):
                formatted[stat] = {}
            formatted[stat][player_tuple] = value

    df = pd.DataFrame(formatted)
    return df

TASKS = [
  training_wr,
]
