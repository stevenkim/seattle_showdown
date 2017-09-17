from dvoa_crap import qb_passing_dvoa_weekly_task
import base
import nfldb
import pandas as pd

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
    db = nfldb.connect()
    q = (nfldb.Query(db)
        .game(season_year=period.season_year,
            week=period.week, season_type='Regular')
        .player(position='QB')
        .as_play_players())
    for play_player in q:
        print play_player


qb_passing_dvoa_agg_player_task = base.NFLDBQueryTask(
    id='qb_passing_dvoa_agg_player_task',
    dependencies=[
        #qb_passing_dvoa_agg_task,
    ],
    nfldb_func=qb_passing_dvoa_agg_player_func,
    csv='dvoa_passing_agg_player.csv',
)

TASKS=[
    qb_passing_dvoa_agg_task,
    qb_passing_dvoa_agg_player_task,
]
