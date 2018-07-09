from core.tasks import pandas_task
from scrapers import fantasy_sharks_wr
from trainers import train_wr_receiving_yds_linear_regression

import pandas as pd
import pickle

@pandas_task('wr_receiving_yds_predict.csv')
def wr_receiving_yds_predict(period):
    model = pickle.load(open(period.offset(-1).get_data_file(
        'wr.receiving_yds.linear_regression.model')))
    df = pd.read_csv(period.get_data_file('training_wr.csv'),
        index_col=['player_id', 'player_name'])
    df = df.fillna(0.0)

    test = df.drop([
        'receiving_tar',
        'receiving_yds',
        'receiving_rec',
        'receiving_tds',
        'rushing_yds',
        'rushing_tds',
        'rushing_att',
        'def',
        'day_of_week',
    ], axis=1)
    pred = model.predict(test)
    df['pred'] = pred
    print df[['pred', 'receiving_yds']]


TASKS=[
    wr_receiving_yds_predict.depends_on(
        fantasy_sharks_wr,
        train_wr_receiving_yds_linear_regression.date_period_offset(-1)),
]
