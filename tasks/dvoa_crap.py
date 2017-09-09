import base
import json
import nfldb
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
    if play.down == 0:
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


def compute_qb_passing_dvoa():
    db = nfldb.connect()
    q = (nfldb.Query(db)
        .game(season_year=base.CONTEXT['season_year'],
            week=base.CONTEXT['week'], season_type='Regular')
        .player(position='QB')
        #.limit(10)
        .as_play_players())
    play_key_tuples = {} # holds play_key => [total_ff_score, total]

    df = pd.DataFrame(columns=[
        'down',
        'yardline',
        'yards_to_go',
        'count',
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
            1,
            fantasy_points(play_player),
        ]
        index += 1
    df = df.groupby(['down', 'yardline', 'yards_to_go']).sum()
    df['avg_fantasy_points'] = df['fantasy_points'] / df['count']
    return df

qb_passing_dvoa = base.NFLDBQueryTask(
    nfldb_func=compute_qb_passing_dvoa,
    csv='passing_avg.csv',
)
