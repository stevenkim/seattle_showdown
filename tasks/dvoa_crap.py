import base
import json
import nfldb
import numpy as np
import pandas as pd
import pprint
from utils import classify_play, fantasy_points


def get_dataframe(period, number_of_weeks, position):
    db = nfldb.connect()
    df = pd.DataFrame(columns=[
        'player_id',
        'player_name',
        'week',
        'def',
        'down',
        'yardline',
        'yards_to_go',
        'passing_att',
        'passing_cmp',
        'passing_yds',
        'passing_tds',
        'passing_int',
        'passing_cmp_air_yds',
        'receiving_rec',
        'receiving_tar',
        'receiving_tds',
        'receiving_yds',
        'receiving_yac_yds',
        'rushing_yds',
        'rushing_tds',
        'rushing_att',
        'fantasy_points',
    ])
    index = 0
    for i in range(number_of_weeks):
        base.info('aggregating week %d', period.week)
        q = (nfldb.Query(db)
            .game(season_year=period.season_year,
                week=period.week, season_type='Regular')
            .player(position=position)
            .as_play_players())

        period = period.offset(-1)
        for play_player in q:
            classified = classify_play(play_player.play)
            if not classified:
                continue

            drive = play_player.play.drive
            if drive.pos_team == drive.game.away_team:
                defense = drive.game.home_team
            else:
                defense = drive.game.away_team
            df.loc[index] = [
                play_player.player.player_id,
                play_player.player.full_name,
                period.week,
                defense,
                classified['down'],
                classified['yardline'],
                classified['yards_to_go'],
                float(play_player.passing_att),
                float(play_player.passing_cmp),
                float(play_player.passing_yds),
                float(play_player.passing_tds),
                float(play_player.passing_int),
                float(play_player.passing_cmp_air_yds),
                float(play_player.receiving_rec),
                float(play_player.receiving_tar),
                float(play_player.receiving_tds),
                float(play_player.receiving_yds),
                float(play_player.receiving_yac_yds),
                float(play_player.rushing_yds),
                float(play_player.rushing_tds),
                float(play_player.rushing_att),
                fantasy_points(play_player),
            ]
            index += 1
    return df

def _position_averages(period, number_of_weeks, position):
    df = _get_dataframe(period, number_of_weeks, position)
    df = df.groupby(['down', 'yardline', 'yards_to_go']).agg({
        'passing_att': ['mean', 'std'],
        'passing_cmp': ['mean', 'std'],
        'passing_yds': ['mean', 'std'],
        'passing_tds': ['mean', 'std'],
        'passing_int': ['mean', 'std'],
        'passing_cmp_air_yds': ['mean', 'std'],
        'receiving_rec': ['mean', 'std'],
        'receiving_tar': ['mean', 'std'],
        'receiving_tds': ['mean', 'std'],
        'receiving_yds': ['mean', 'std'],
        'receiving_yac_yds': ['mean', 'std'],
        'rushing_yds': ['mean', 'std'],
        'rushing_tds': ['mean', 'std'],
        'rushing_att': ['mean', 'std'],
        'fantasy_points': ['mean', 'count', 'std'],
    })
    df.columns = ["_".join(x) for x in df.columns.ravel()]
    return df

def _position_averages_by_player(period, number_of_weeks, position):
    avg = pd.read_csv(period.get_data_file(position.lower()+'_averages_17.csv'))
    players = _get_dataframe(period, number_of_weeks, position)

    players = pd.merge(players, avg, on=['down', 'yardline', 'yards_to_go'],
        suffixes=('', '_expected'))

    stats = [
        'passing_att',
        'passing_cmp',
        'passing_yds',
        'passing_tds',
        'passing_int',
        'passing_cmp_air_yds',
        'receiving_rec',
        'receiving_tar',
        'receiving_tds',
        'receiving_yds',
        'receiving_yac_yds',
        'rushing_yds',
        'rushing_tds',
        'rushing_att',
        'fantasy_points',
    ]

    aggs = {}
    for stat in stats:
        players[stat+'_diff'] = players[stat] - players[stat+'_mean']

        if stat == 'fantasy_points':
            aggs[stat] = ['mean', 'count', 'std']
            aggs[stat+'_diff'] = ['mean', 'count', 'std']
        else:
            aggs[stat] = ['mean', 'std']
            aggs[stat+'_diff'] = ['mean', 'std']


    players = players.groupby([
        'player_id',
        'player_name',
        'down',
        'yardline',
        'yards_to_go',
    ]).agg(aggs)
    players.columns = ["_".join(x) for x in players.columns.ravel()]
    return players

@base.pandas_task('qb_averages_17.csv')
def qb_averages_17(period):
    '''
    QB Averages across all players. Used as a baseline
    '''
    return _position_averages(period, 17, 'QB')

@base.pandas_task('qb_averages_by_player_17.csv')
def qb_averages_by_player_17(period):
    '''
    QB Averages for each player for that past season's worth of games
    '''
    return _position_averages_by_player(period, 17, 'QB')

@base.pandas_task('qb_fdvoa_17.csv')
def qb_fdvoa_17(period):
    qbs = pd.read_csv(period.get_data_file('qb_averages_by_player_17.csv'))

    qbs['fdvoa'] = qbs['fantasy_points_diff_mean'] * \
        qbs['fantasy_points_count']
    qbs['fdvoa_std'] = qbs['fantasy_points_diff_std'] * \
        qbs['fantasy_points_diff_std']

    sums = qbs.groupby(['player_name', 'player_id']).agg({
        'fdvoa': 'sum',
        'fdvoa_std': 'sum',
        'fantasy_points_count': 'sum',
    })
    sums['fdvoa'] = sums['fdvoa'] / sums['fantasy_points_count']
    sums['fdvoa_std'] = np.sqrt(sums['fdvoa_std'])

    return sums


TASKS=[
    qb_fdvoa_17.depends_on(
        qb_averages_by_player_17.depends_on(
            qb_averages_17,
        ),
    ),
]
