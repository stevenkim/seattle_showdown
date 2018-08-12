from core.tasks import pandas_task

import nfldb
import pandas as pd
import utils

@pandas_task('training_wr.csv')
def training_wr(period):
    db = nfldb.connect()

    current_period = period
    data = {}

    start_period = period.offset(-10)
    team_def_data = {}
    team_off_data = {}
    with nfldb.Tx(db) as cursor:
        cursor.execute('''
            SELECT p.pos_team AS team,
                CASE WHEN p.pos_team = g.home_team THEN g.away_team
                    ELSE g.home_team
                    END AS def_team,
                %d - (g.season_year * 17 + g.week) as week_offset,
                g.season_year,
                g.week,
                sum(ap.passing_yds) AS passing_yds,
                sum(ap.passing_att) AS passing_att,
                sum(ap.passing_cmp) AS passing_cmp,
                sum(ap.passing_tds) AS passing_tds,
                sum(ap.passing_int) AS passing_int,
                count(distinct p.play_id) AS total_plays
            FROM agg_play ap,
                play p,
                game g
            WHERE g.gsis_id = p.gsis_id
                AND g.gsis_id = ap.gsis_id
                AND p.play_id = ap.play_id
                AND (g.season_year * 100 + g.week) BETWEEN %d AND %d
                AND g.season_type='Regular'
            GROUP By 1, 2, 3, 4, 5
        ''' % (period.season_year * 17 + period.week,
            start_period.season_year * 100 + start_period.week,
            period.season_year * 100 + period.week))
        for row in cursor.fetchall():
            if row['def_team'] not in team_def_data:
                team_def_data[row['def_team']] = {}
            offset = str(row['week_offset'])
            team_def_data[row['def_team']].update({
                'def_passing_yds_'+offset: row['passing_yds'],
                'def_passing_att_'+offset: row['passing_att'],
                'def_passing_cmp_'+offset: row['passing_cmp'],
                'def_passing_tds_'+offset: row['passing_tds'],
                'def_passing_int_'+offset: row['passing_int'],
                'def_total_plays_'+offset: row['total_plays'],
            })
            if row['team'] not in team_off_data:
                team_off_data[row['team']] = {}
            team_off_data[row['team']].update({
                'off_passing_yds_'+offset: row['passing_yds'],
                'off_passing_att_'+offset: row['passing_att'],
                'off_passing_cmp_'+offset: row['passing_cmp'],
                'off_passing_tds_'+offset: row['passing_tds'],
                'off_passing_int_'+offset: row['passing_int'],
                'off_total_plays_'+offset: row['total_plays'],
            })

    with nfldb.Tx(db) as cursor:
        cursor.execute('''
            SELECT p.pos_team AS team,
                CASE WHEN p.pos_team = g.home_team THEN g.away_team
                    ELSE g.home_team
                    END AS def_team,
                %d - (g.season_year * 17 + g.week) as week_offset,
                g.season_year,
                g.week,
                player.full_name,
                player.player_id,
                sum(pp.receiving_tar) AS receiving_tar,
                sum(pp.receiving_yds) AS receiving_yds,
                sum(pp.receiving_rec) AS receiving_rec,
                sum(pp.receiving_tds) AS receiving_tds,
                sum(pp.rushing_yds) AS rushing_yds,
                sum(pp.rushing_tds) AS rushing_tds,
                sum(pp.rushing_att) AS rushing_att
            FROM game g,
                play p,
                play_player pp,
                player player
            WHERE g.gsis_id = p.gsis_id
                AND g.gsis_id = pp.gsis_id
                AND p.play_id = pp.play_id
                AND pp.player_id = player.player_id
                AND (g.season_year * 100 + g.week) BETWEEN %d AND %d
                AND g.season_type='Regular'
                AND player.position = 'WR'
            GROUP By 1, 2, 3, 4, 5, 6, 7
        ''' % (period.season_year * 17 + period.week,
            start_period.season_year * 100 + start_period.week,
            period.season_year * 100 + period.week))
        for row in cursor.fetchall():
            player_id = row['player_id']
            full_name = row['full_name']
            tuple = (player_id, full_name)
            if tuple not in data:
                data[tuple] = {}
            offset = str(row['week_offset'])
            data[tuple].update({
                'receiving_tar_'+offset: row['receiving_tar'],
                'receiving_yds_'+offset: row['receiving_yds'],
                'receiving_rec_'+offset: row['receiving_rec'],
                'receiving_tds_'+offset: row['receiving_tds'],
                'rushing_yds_'+offset: row['rushing_yds'],
                'rushing_tds_'+offset: row['rushing_tds'],
                'rushing_att_'+offset: row['rushing_att'],
            })
            if offset == '0':
                data[tuple].update(team_def_data[row['def_team']])
                data[tuple].update(team_off_data[row['team']])

    formatted = {}
    for player_tuple, stats in data.items():
        for stat, value in stats.items():
            if not formatted.has_key(stat):
                formatted[stat] = {}
            formatted[stat][player_tuple] = value

    df = pd.DataFrame(formatted)
    df.index.set_names(['player_id', 'player_name'], inplace=True)
    return df

TASKS = [
  training_wr,
]
