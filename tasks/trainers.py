from core.tasks import scikit_task
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

import math
import numpy as np
import pandas as pd

def get_dataframe(period):
    df = pd.read_csv(period.get_data_file('training_wr.csv'),
        index_col=['player_id', 'player_name'])

    for i in range(16):
        period = period.offset(-1)
        df = df.append(pd.read_csv(period.get_data_file('training_wr.csv'),
            index_col=['player_id', 'player_name']))
    df = df.fillna(0.0)
    return df

def train_model(model, period):
    df = get_dataframe(period)
    df = df.drop(['def', 'day_of_week'], axis=1)

    mask = np.random.rand(len(df)) < .8
    training = df[mask]
    test = df[~mask]

    training_y = training['receiving_yds']
    training_x = training.drop('receiving_yds', axis=1)

    model = linear_model.LinearRegression()
    model.fit(training_x, training_y)

    test = test[test['receiving_yds'] > 10]
    test_y = test['receiving_yds']
    test_x = test.drop('receiving_yds', axis=1)

    pred_y = model.predict(test_x)

    results = pd.DataFrame(test_y.reset_index())
    results['pred_y'] = pred_y
    print results

    print "Mean squared error: %.2f" % mean_squared_error(test_y, pred_y)
    print "RMSE: %.2f" % math.sqrt(mean_squared_error(test_y, pred_y))
    print "Variance score: %.2f" % r2_score(test_y, pred_y)

    return model

@scikit_task('wr.receiving_yds.linear_regression')
def train_wr_receiving_yds(period):
    return train_model(linear_model.LinearRegression(), period)


TASKS = [
    train_wr_receiving_yds,
]
