import base
import json
import nfldb
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
        return 'crap_field_position'

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
        .game(season_year=2016, week=1, season_type='Regular')
        .player(position='QB')
        #.limit(100)
        .as_play_players())
    play_key_tuples = {} # holds play_key => tuple(total_ff_score, total)
    for play_player in q:
        classified = classify_play(play_player.play)
        if not classified:
            continue

        play_key = json.dumps(classified)

        if play_key not in play_key_tuples:
            play_key_tuples[play_key] = [0.0, 0]
        play_key_tuples[play_key][0] += fantasy_points(play_player)
        play_key_tuples[play_key][1] += 1

    for play_key in play_key_tuples:
        # get the average fantasy points
        play_key_tuples[play_key].append(
            play_key_tuples[play_key][0] /
            play_key_tuples[play_key][1])

    pprint.pprint(play_key_tuples)
    print(len(play_key_tuples))



qb_passing_dvoa = base.NFLDBQueryTask(
    nfldb_func=compute_qb_passing_dvoa,
)
