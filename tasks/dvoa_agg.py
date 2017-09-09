import base
import pandas as pd

def aggregate_qb_dvoa(period):
    # start at season 2016
    frames = []
    while period.season_year >= 2016 and period.week >= 1:
        frames.append(pd.read_csv(
            period.get_data_file('dvoa_passing_avg_weekly.csv')))
        period = period.offset(-1)
    df = pd.concat(frames).groupby(['down', 'yardline', 'yards_to_go']).sum()
    df['avg_fantasy_points'] = df['fantasy_points'] / df['count']
    print df
    return df


qb_passing_dvoa_agg = base.NFLDBQueryTask(
    nfldb_func=aggregate_qb_dvoa,
    csv='dvoa_passing_agg.csv',
)
