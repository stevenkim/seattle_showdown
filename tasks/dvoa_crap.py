import base
import json
import nfldb
import numpy as np
import pandas as pd
import pprint


def fantasy_points(play_player):
    per_yard_config = {
        'passing_yds': 1/25.0,
        'passing_tds': 4.0,
        'passing_int': -2.0,
    }

    total = 0

    for stat in per_yard_config:
        total += getattr(play_player, stat) * per_yard_config[stat]

    return total

def classify_play(play):
    # skip special downs like TO or 2 pt conversion
    if play.down == 0 or play.down is None:
        return None

    def normalize_yardage(yardline):
        offset = yardline._offset
        if offset >= 40:
            return 'goaline'
        if offset >= 30:
            return 'redzone'
        if offset >= 20:
            return 'fg'
        if offset >= -20:
            return 'midfield'
        if offset >= -30:
            return 'touchback'
        return 'crap'

    def normalize_yards_to_go(yards_to_go):
        if yards_to_go <= 3:
            return 'short'
        if yards_to_go <= 6:
            return 'medium'
        if yards_to_go <= 10:
            return 'long'
        return 'really_long'

    return {
        'down': play.down,
        'yardline': normalize_yardage(play.yardline),
        'yards_to_go': normalize_yards_to_go(play.yards_to_go),
    }

def compute_qb_passing_dvoa(period):
    db = nfldb.connect()
    q = (nfldb.Query(db)
        .game(season_year=period.season_year,
            week=period.week, season_type='Regular')
        .player(position='QB')
        #.limit(10)
        .as_play_players())

    df = pd.DataFrame(columns=[
        'down',
        'yardline',
        'yards_to_go',
        'fantasy_points'
    ])
    index = 0
    for play_player in q:
        classified = classify_play(play_player.play)
        if not classified:
            continue

        df.loc[index] = [
            classified['down'],
            classified['yardline'],
            classified['yards_to_go'],
            fantasy_points(play_player),
        ]
        index += 1
    df = df.groupby(['down', 'yardline', 'yards_to_go']).agg(
        ['count', 'std', 'mean'])
    return df

qb_passing_dvoa_weekly_task = base.NFLDBQueryTask(
    id='qb_passing_dvoa_weekly_task',
    nfldb_func=compute_qb_passing_dvoa,
    csv='dvoa_passing_avg_weekly.csv',
)

def qb_passing_dvoa_agg_func(period):
    # start at season 2016
    frames = []
    while period.season_year >= 2016 and period.week >= 1:
        frames.append(pd.read_csv(
            period.get_data_file('dvoa_passing_avg_weekly.csv')))
        period = period.offset(-1)
    df = pd.concat(frames).groupby(['down', 'yardline', 'yards_to_go']).sum()
    df['avg_fantasy_points'] = df['fantasy_points'] / df['count']
    return df


qb_passing_dvoa_agg_task = base.NFLDBQueryTask(
    id='qb_passing_dvoa_task',
    dependencies=[
        #qb_passing_dvoa_weekly_task.date_period_offset(-1),
    ],
    nfldb_func=qb_passing_dvoa_agg_func,
    csv='dvoa_passing_agg.csv',
)

def qb_passing_dvoa_agg_player_func(period):
    '''
    compute each player's fantasy points and its difference from the average
    '''
    db = nfldb.connect()
    q = (nfldb.Query(db)
        .game(season_year=period.season_year,
            week=period.week, season_type='Regular')
        .player(position='QB')
        .limit(10)
        .as_play_players())

    avg = pd.read_csv(period.get_data_file('dvoa_passing_agg.csv'),
        index_col=['down', 'yardline', 'yards_to_go'])

    df = pd.DataFrame(columns=[
        'player_id',
        'player_name',
        'down',
        'yardline',
        'yards_to_go',
        'count',
        'fantasy_points',
    ])
    index = 0
    for play_player in q:
        classified = classify_play(play_player.play)
        if not classified:
            continue

        df.loc[index] = [
            play_player.player.player_id,
            play_player.player.full_name,
            classified['down'],
            classified['yardline'],
            classified['yards_to_go'],
            1,
            fantasy_points(play_player),
        ]
        index += 1

    print df
    df = df.groupby([
        'player_id',
        'player_name',
        'down',
        'yardline',
        'yards_to_go',
    ]).sum()

    print df.loc[(slice(None), slice(None), slice('1')), :]


qb_passing_dvoa_agg_player_task = base.NFLDBQueryTask(
    id='qb_passing_dvoa_agg_player_task',
    dependencies=[
        #qb_passing_dvoa_agg_task,
    ],
    nfldb_func=qb_passing_dvoa_agg_player_func,
    csv='dvoa_passing_agg_player.csv',
)

TASKS=[
    qb_passing_dvoa_weekly_task,
    # qb_passing_dvoa_agg_task,
    #qb_passing_dvoa_agg_player_task,
]
