from core.tasks import pandas_task

import json
import pandas as pd
import urllib2

def fantasy_sharks(pos):
    url = ('https://www.fantasysharks.com/apps/Projections/'
        'WeeklyProjections.php?pos=%s&format=json' % pos)
    request = urllib2.Request(url, headers={
        'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
    })
    content = urllib2.urlopen(request).read()
    players = json.loads(content)

    df = pd.DataFrame(players)
    return df

@pandas_task('fantasy_sharks_qb.csv')
def fantasy_sharks_qb(period):
    return fantasy_sharks('qb')

@pandas_task('fantasy_sharks_rb.csv')
def fantasy_sharks_rb(period):
    return fantasy_sharks('rb')

@pandas_task('fantasy_sharks_wr.csv')
def fantasy_sharks_wr(period):
    return fantasy_sharks('wr')

@pandas_task('fantasy_sharks_te.csv')
def fantasy_sharks_te(period):
    return fantasy_sharks('te')

TASKS = [
    fantasy_sharks_qb,
    fantasy_sharks_rb,
    fantasy_sharks_wr,
    fantasy_sharks_te,
]
