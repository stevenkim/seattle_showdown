
def fantasy_points(play_player):
    per_yard_config = {
        'passing_yds': 1/25.0,
        'passing_tds': 4.0,
        'passing_int': -2.0,
        'rushing_yds': 1/10.0,
        'rushing_tds': 6.0,
        'receiving_yds': 1/10.0,
        'receiving_tds': 6.0,
        'fumbles_lost': -2.0,
    }

    total = 0

    for stat in per_yard_config:
        total += getattr(play_player, stat) * per_yard_config[stat]

    return total

def classify_play(play):
    # skip special downs like TO or 2 pt conversion
    if play.down == 0 or play.down is None:
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
