class BaseModel:
    def __init__(self, **kwargs):
        s = set(self.FIELDS)
        for key, value in kwargs.iteritems():
            assert key in s, key + " was not defined in FIELDS dummy"

        self.data = kwargs

    @classmethod
    def getHeaderCSVRow(cls):
        return ",".join(cls.FIELDS)

    def toCSVRow(self):
        row = []
        for field in self.FIELDS:
            row.append(str(self.data.get(field, '')))
        return ",".join(row)

    def serialize(self, fp):
        fp.write(self.toCSVRow())
        fp.write("\n")


class Team(BaseModel):
    FIELDS = [
        "id",
    ]

    ID_TO_OWNER = {
        1: "Andrew",
        2: "Steve",
        3: "Homan",
        4: "Charlie",
        7: "Phil",
        8: "Nick",
        10: "Daniel",
        11: "ChiBong",
        12: "Paul",
        13: "Mike",
    }


class Game(BaseModel):
    FIELDS = [
        "id",
        "team1_id",
        "team2_id",
    ]


class PlayerGameStats(BaseModel):
    FIELDS = [
        "week",
        "player_id",
        "game_id",
        "team_id",
        "slot",
        "player_name",
        "player_pos",
        "pass_comps",
        "pass_atts",
        "pass_yards",
        "pass_tds",
        "pass_ints",
        "rush_attempts",
        "rush_yards",
        "rush_tds",
        "receptions",
        "rec_yards",
        "rec_tds",
        "rec_targets",
        "two_pt",
        "fumbles_lost",
        "misc_td",
        "fgm_1_39",
        "fga_1_39",
        "fgm_40_49",
        "fga_40_49",
        "fgm_50",
        "fga_50",
        "fgm",
        "fga",
        "fgm_xp",
        "fga_xp",
        "def_tds",
        "def_ints",
        "def_fumbles_rec",
        "def_sacks",
        "def_safeties",
        "def_blocks",
        "def_points_allowed",
        "total_points",
    ]
