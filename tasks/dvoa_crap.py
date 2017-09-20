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
        'rushing_yds': 1/10.0,
        'rushing_tds': 6.0,
        'receiving_yds': 1/10.0,
        'receiving_tds': 6.0,
        'fumbles_lost': -2.0,
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

@base.pandas_task('qb_weekly_averages.csv')
def qb_weekly_averages(period):
    db = nfldb.connect()
    q = (nfldb.Query(db)
        .game(season_year=period.season_year,
            week=period.week, season_type='Regular')
        .player(position='QB')
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

def get_qb_dataframe(period):
    db = nfldb.connect()
    df = pd.DataFrame(columns=[
        'player_id',
        'player_name',
        'down',
        'yardline',
        'yards_to_go',
        'passing_att',
        'passing_cmp',
        'passing_yds',
        'passing_tds',
        'passing_int',
        'passing_cmp_air_yds',
        'fantasy_points',
    ])
    index = 0
    for i in range(17):
        base.info('aggregating week %d', period.week)
        q = (nfldb.Query(db)
            .game(season_year=period.season_year,
                week=period.week, season_type='Regular')
            .player(position='QB')
            .as_play_players())

        period = period.offset(-1)
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
                float(play_player.passing_att),
                float(play_player.passing_cmp),
                float(play_player.passing_yds),
                float(play_player.passing_tds),
                float(play_player.passing_int),
                float(play_player.passing_cmp_air_yds),
                fantasy_points(play_player),
            ]
            index += 1
    return df

@base.pandas_task('qb_season_averages.csv')
def qb_season_averages(period):
    '''
    QB Averages across all players. Used as a baseline
    '''
    df = get_qb_dataframe(period)
    df = df.groupby(['down', 'yardline', 'yards_to_go']).agg({
        'passing_att': ['mean', 'std'],
        'passing_cmp': ['mean', 'std'],
        'passing_yds': ['mean', 'std'],
        'passing_tds': ['mean', 'std'],
        'passing_int': ['mean', 'std'],
        'passing_cmp_air_yds': ['mean', 'std'],
        'fantasy_points': ['mean', 'count', 'std'],
    })
    df.columns = ["_".join(x) for x in df.columns.ravel()]
    return df

@base.pandas_task('qb_season_averages_by_player.csv')
def qb_season_averages_by_player(period):
    '''
    QB Averages for each player for that past season's worth of games
    '''
    df = get_qb_dataframe(period)
    df = df.groupby([
        'player_id',
        'player_name',
        'down',
        'yardline',
        'yards_to_go',
    ]).agg({
        'passing_att': ['mean', 'std'],
        'passing_cmp': ['mean', 'std'],
        'passing_yds': ['mean', 'std'],
        'passing_tds': ['mean', 'std'],
        'passing_int': ['mean', 'std'],
        'passing_cmp_air_yds': ['mean', 'std'],
        'fantasy_points': ['mean', 'count', 'std'],
    })
    df.columns = ["_".join(x) for x in df.columns.ravel()]
    return df

@base.pandas_task('qb_season_dvoa.csv')
def qb_season_dvoa(period):
    avg = pd.read_csv(period.get_data_file('qb_season_averages.csv'))
    qbs = pd.read_csv(period.get_data_file(
        'qb_season_averages_by_player.csv'))

    columns = [
        'passing_att',
        'passing_cmp',
        'passing_yds',
        'passing_tds',
        'passing_int',
        'passing_cmp_air_yds',
        'fantasy_points',
    ]
    merged = pd.merge(qbs, avg, on=['down', 'yardline', 'yards_to_go'])
    # only calculate fantasy points for now

    merged['fdvoa'] = (merged['fantasy_points_mean_x'] -
        merged['fantasy_points_mean_y']) * merged['fantasy_points_count_x']

    sums = merged.groupby(['player_name', 'player_id']).agg({
        'fdvoa': 'sum',
        'fantasy_points_count_x': 'sum',
    })
    sums['fdvoa_avg'] = sums['fdvoa'] / sums['fantasy_points_count_x']

    '''
    for column in columns:
        merged[column + '_z'] = (merged[column + '_mean_x'] - merged[column + '_mean_y']) / \
            np.sqrt(
                (np.square(merged[column + '_std_x']) / merged['fantasy_points_count_x']) +
                (np.square(merged[column + '_std_y']) / merged['fantasy_points_count_y'])
            )
    '''

    return sums


TASKS=[
    qb_season_dvoa.depends_on(
        qb_season_averages,
        qb_season_averages_by_player,
    ),
]
