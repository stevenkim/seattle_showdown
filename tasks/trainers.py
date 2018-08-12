from core.tasks import scikit_task
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score, cross_val_predict
from training_data import training_wr

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
    target = df['receiving_yds_0']
    # remove this weeks data for training duh
    training_columns = [c for c in df.columns if not c.endswith('_0')]
    training = df[training_columns]

    predicted = cross_val_predict(model, training, target)
    results = pd.DataFrame(target.reset_index())
    results['pred_y'] = predicted
    # print results

    print model
    print "Mean squared error: %.2f" % mean_squared_error(target, predicted)
    print "RMSE: %.2f" % math.sqrt(mean_squared_error(target, predicted))
    print "Variance score: %.2f\n" % r2_score(target, predicted)
    return model

@scikit_task('wr.receiving_yds.linear_regression')
def train_wr_receiving_yds_linear_regression(period):
    return train_model(linear_model.LinearRegression(), period)

@scikit_task('wr.receiving_yds.ridge')
def train_wr_receiving_yds_ridge(period):
    return train_model(linear_model.RidgeCV(), period)

@scikit_task('wr.receiving_yds.lasso')
def train_wr_receiving_yds_lasso(period):
    return train_model(linear_model.Lasso(), period)

@scikit_task('wr.receiving_yds.logistic_regression')
def train_wr_receiving_yds_logistic_regression(period):
    return train_model(linear_model.LogisticRegression(), period)

training_data_tasks = [training_wr.date_period_offset(-1 * x) for x in range(0, 17)]
TASKS = [
    train_wr_receiving_yds_linear_regression.depends_on(*training_data_tasks),
    train_wr_receiving_yds_logistic_regression.depends_on(*training_data_tasks),
    train_wr_receiving_yds_ridge.depends_on(*training_data_tasks),
    train_wr_receiving_yds_lasso.depends_on(*training_data_tasks),
]
