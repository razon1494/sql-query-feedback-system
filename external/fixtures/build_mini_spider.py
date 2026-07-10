"""
Build a tiny Spider-shaped fixture for testing the external pipeline WITHOUT
the 1.4 GB download.

It reproduces Spider's on-disk layout exactly:

    external/fixtures/mini_spider/
        tables.json
        dev.json
        database/music_mini/music_mini.sqlite

The schema is deliberately a *music* database — nothing like the seven-table
university schema — so a passing test proves the detector is genuinely
schema-independent, not quietly relying on university table/column names.

Run directly to (re)build, or import `build()` from tests.
"""
import os
import json
import sqlite3

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mini_spider")
DB_ID = "music_mini"


def _db_path(root: str) -> str:
    return os.path.join(root, "database", DB_ID, f"{DB_ID}.sqlite")


def _build_sqlite(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE artist (
            artist_id INTEGER PRIMARY KEY,
            name      TEXT,
            country   TEXT
        );
        CREATE TABLE album (
            album_id  INTEGER PRIMARY KEY,
            title     TEXT,
            artist_id INTEGER REFERENCES artist(artist_id),
            year      INTEGER,
            genre     TEXT
        );
        CREATE TABLE track (
            track_id  INTEGER PRIMARY KEY,
            album_id  INTEGER REFERENCES album(album_id),
            title     TEXT,
            seconds   INTEGER
        );
        CREATE TABLE playlist (
            playlist_id INTEGER PRIMARY KEY,
            name        TEXT
        );
        CREATE TABLE playlist_track (
            playlist_id INTEGER REFERENCES playlist(playlist_id),
            track_id    INTEGER REFERENCES track(track_id)
        );
        """
    )
    artists = [
        (1, "The Beatles", "UK"),
        (2, "Queen", "UK"),
        (3, "Nirvana", "US"),
    ]
    albums = [
        (1, "Abbey Road", 1, 1969, "rock"),
        (2, "Revolver", 1, 1966, "rock"),
        (3, "A Night at the Opera", 2, 1975, "rock"),
        (4, "Nevermind", 3, 1991, "grunge"),
        # Album with no tracks: makes LEFT-vs-INNER join divergence observable
        # (needed by the wrong-query M4 operator test).
        (5, "A Day at the Races", 2, 1976, "rock"),
    ]
    tracks = [
        (1, 1, "Come Together", 259),
        (2, 1, "Something", 182),
        (3, 1, "Octopus's Garden", 171),
        (4, 2, "Taxman", 159),
        (5, 2, "Eleanor Rigby", 128),
        (6, 3, "Bohemian Rhapsody", 354),
        (7, 4, "Smells Like Teen Spirit", 301),
    ]
    # M:N structure for division problems: track 1 and track 6 appear on every
    # playlist (division answer); track 2 is a partial match (the IN-based
    # wrong query must diverge on it).
    playlists = [(1, "Morning"), (2, "Drive")]
    playlist_tracks = [(1, 1), (1, 2), (1, 6), (2, 1), (2, 6)]
    cur.executemany("INSERT INTO artist VALUES (?,?,?)", artists)
    cur.executemany("INSERT INTO album VALUES (?,?,?,?,?)", albums)
    cur.executemany("INSERT INTO track VALUES (?,?,?,?)", tracks)
    cur.executemany("INSERT INTO playlist VALUES (?,?)", playlists)
    cur.executemany("INSERT INTO playlist_track VALUES (?,?)", playlist_tracks)
    conn.commit()
    conn.close()


# A few gold (correct) queries spanning problem types, in Spider's example shape.
DEV_EXAMPLES = [
    {
        "db_id": DB_ID,
        "question": "List each artist name together with the title of each of their albums.",
        "query": "SELECT a.name, al.title FROM artist a JOIN album al ON a.artist_id = al.artist_id",
        "_problem_type": "JOIN",
    },
    {
        "db_id": DB_ID,
        "question": "Show each artist who has more than one album, with their album count.",
        "query": ("SELECT a.name, COUNT(al.album_id) AS n FROM artist a "
                  "JOIN album al ON a.artist_id = al.artist_id "
                  "GROUP BY a.artist_id, a.name HAVING COUNT(al.album_id) > 1"),
        "_problem_type": "AGGREGATION",
    },
    {
        "db_id": DB_ID,
        "question": "Titles of albums by the artist named 'Queen'.",
        "query": ("SELECT title FROM album "
                  "WHERE artist_id = (SELECT artist_id FROM artist WHERE name = 'Queen')"),
        "_problem_type": "SUBQUERY",
    },
    {
        "db_id": DB_ID,
        "question": "Names of artists from the UK or the US.",
        "query": ("SELECT name FROM artist WHERE country = 'UK' "
                  "UNION SELECT name FROM artist WHERE country = 'US'"),
        "_problem_type": "SET_OP",
    },
]


def _minimal_tables_json() -> list:
    # Spider's tables.json is richer than this, but the pipeline reads schema
    # from the .sqlite via PRAGMA. We emit a minimal, valid-enough entry so the
    # fixture mirrors the real layout (a tables.json file is present).
    return [{
        "db_id": DB_ID,
        "table_names_original": ["artist", "album", "track"],
        "table_names": ["artist", "album", "track"],
        "column_names_original": [
            [-1, "*"],
            [0, "artist_id"], [0, "name"], [0, "country"],
            [1, "album_id"], [1, "title"], [1, "artist_id"], [1, "year"], [1, "genre"],
            [2, "track_id"], [2, "album_id"], [2, "title"], [2, "seconds"],
        ],
        "column_types": [
            "number", "text", "text",
            "number", "text", "number", "number", "text",
            "number", "number", "text", "number",
        ],
        "primary_keys": [1, 4, 9],
        "foreign_keys": [[6, 1], [10, 4]],
    }]


def build(root: str = FIXTURE_DIR) -> str:
    """Build the fixture under `root` and return the root path."""
    os.makedirs(root, exist_ok=True)
    _build_sqlite(_db_path(root))
    with open(os.path.join(root, "dev.json"), "w", encoding="utf-8") as fh:
        json.dump(DEV_EXAMPLES, fh, indent=1)
    with open(os.path.join(root, "tables.json"), "w", encoding="utf-8") as fh:
        json.dump(_minimal_tables_json(), fh, indent=1)
    return root


if __name__ == "__main__":
    path = build()
    print(f"Mini-Spider fixture built at: {path}")
    print(f"  database: {_db_path(path)}")
    print(f"  examples: {len(DEV_EXAMPLES)}")
