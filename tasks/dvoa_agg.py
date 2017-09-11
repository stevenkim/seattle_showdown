from dvoa_crap import qb_passing_dvoa_weekly_task
import base
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

TASKS=[
    qb_passing_dvoa_agg_task,
]
