# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest TMDB Movies -> Lakebase
# MAGIC
# MAGIC Pipeline for **Capstone #1 (AI Movie Night Planner)**. It pulls movie data
# MAGIC from the [TMDB API](https://developer.themoviedb.org/reference/intro/getting-started)
# MAGIC and loads it into Lakebase (Databricks-managed Postgres).
# MAGIC
# MAGIC It reuses the project's existing `lakebase.py` helper (a thin psycopg2
# MAGIC wrapper) for **all** database work - creating the tables and populating
# MAGIC them - so there's no separate connection logic to maintain here.
# MAGIC
# MAGIC Only the tables TMDB can actually supply are created and filled:
# MAGIC
# MAGIC | Table | What it holds |
# MAGIC |-------|---------------|
# MAGIC | `movies` | Core movie row: title, `overview` (plot), release date, runtime, votes, poster path, full `payload` JSONB |
# MAGIC | `genres` | Genre id -> name reference |
# MAGIC | `movie_genres` | movie <-> genre join |
# MAGIC | `people` | Cast members (actors) |
# MAGIC | `movie_cast` | movie <-> person join, with character + billing order |
# MAGIC
# MAGIC The user-generated tables from the capstone spec (`users`, `groups`,
# MAGIC `ratings`, `watchlist_items`, `recommendations`) are **not** created here -
# MAGIC those are written by the app, not the API.
# MAGIC
# MAGIC ### One-time setup: TMDB secret
# MAGIC
# MAGIC The pipeline authenticates with your TMDB **API Read Access Token** (v4
# MAGIC auth - the long `eyJ...` token on your TMDB API settings page), stored in a
# MAGIC Databricks secret so it never lives in code. Create it once by running the
# MAGIC repo's helper script - from a Databricks notebook, or locally with your
# MAGIC Databricks workspace auth configured:
# MAGIC
# MAGIC ```bash
# MAGIC python setup_tmdb_secret.py   # prompts for the read access token
# MAGIC ```
# MAGIC
# MAGIC That stores it in the `tmdb/api-key` secret - the same scheme
# MAGIC `massive_client.py` uses for the Massive key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC
# MAGIC Widgets let you tune the pull without editing the notebook - handy when
# MAGIC running this as a scheduled Databricks Job.

# COMMAND ----------

dbutils.widgets.text("tmdb_secret_scope", "tmdb", "TMDB secret scope")
dbutils.widgets.text("tmdb_secret_key", "api-key", "TMDB secret key (read access token)")
dbutils.widgets.text("tmdb_api_base_url", "https://api.themoviedb.org/3", "TMDB API base URL")
dbutils.widgets.text("num_pages", "10", "Pages to pull per list (20 movies/page)")
dbutils.widgets.text("cast_limit", "15", "Max top-billed cast to keep per movie")
dbutils.widgets.text("requests_per_second", "30", "TMDB request throttle (be polite)")
dbutils.widgets.text("language", "en-US", "TMDB language")

TMDB_SECRET_SCOPE = dbutils.widgets.get("tmdb_secret_scope")
TMDB_SECRET_KEY = dbutils.widgets.get("tmdb_secret_key")
TMDB_API_BASE_URL = dbutils.widgets.get("tmdb_api_base_url").rstrip("/")
NUM_PAGES = int(dbutils.widgets.get("num_pages"))
CAST_LIMIT = int(dbutils.widgets.get("cast_limit"))
REQUESTS_PER_SECOND = float(dbutils.widgets.get("requests_per_second"))
LANGUAGE = dbutils.widgets.get("language")

# The two curated TMDB lists we pull movie ids from. Each movie remembers which
# list(s) it came from via the movies.source_lists array column.
SOURCE_LISTS = ["popular", "top_rated"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reuse the project's helpers: `lakebase.py` + `tmdb_client.py`
# MAGIC
# MAGIC Both live at the repo root. `lakebase.py` opens a psycopg2 connection to
# MAGIC Lakebase (reading the `database/lakebase-url` secret); `tmdb_client.py`
# MAGIC wraps the TMDB API (reading the `tmdb/api-key` secret). We just put the
# MAGIC repo root on `sys.path` so this notebook, which sits in `notebooks/`, can
# MAGIC import them.

# COMMAND ----------

import os
import sys

# Add both the notebook's own dir and its parent (the repo root) so the project
# modules resolve regardless of the workspace's default working directory.
for _p in (os.getcwd(), os.path.abspath(os.path.join(os.getcwd(), ".."))):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import lakebase
    from tmdb_client import TmdbClient
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Could not import project modules (lakebase.py / tmdb_client.py). This "
        "notebook expects to run inside the project repo (Databricks Git folder) "
        f"so the repo root is on sys.path. Checked: {[p for p in sys.path[:2]]}"
    ) from exc

print("Imported helpers:", lakebase.__file__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TMDB client
# MAGIC
# MAGIC `TmdbClient` reads the read access token from the `tmdb/api-key` secret and
# MAGIC handles Bearer auth + throttling. We point it at the widget-configured
# MAGIC scope/key/base-url/language so a scheduled Job can override them.

# COMMAND ----------

client = TmdbClient(
    base_url=TMDB_API_BASE_URL,
    language=LANGUAGE,
    requests_per_second=REQUESTS_PER_SECOND,
    secret_scope=TMDB_SECRET_SCOPE,
    secret_key=TMDB_SECRET_KEY,
)

# Quick auth sanity check.
_sample = client.get_movie_list("popular", page=1)
print(f"TMDB reachable - page 1 of popular has {len(_sample)} movies.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the tables
# MAGIC
# MAGIC All DDL runs through a single `lakebase.get_connection()` - one of the
# MAGIC advantages of psycopg2 here: the notebook creates its own schema, so
# MAGIC there's no separate SQL file to run by hand first.

# COMMAND ----------

DDL = """
CREATE TABLE IF NOT EXISTS movies (
    id                BIGINT PRIMARY KEY,
    title             TEXT NOT NULL,
    original_title    TEXT,
    overview          TEXT,
    tagline           TEXT,
    release_date      DATE,
    runtime           INT,
    status            TEXT,
    original_language TEXT,
    popularity        DOUBLE PRECISION,
    vote_average      DOUBLE PRECISION,
    vote_count        INT,
    budget            BIGINT,
    revenue           BIGINT,
    poster_path       TEXT,
    backdrop_path     TEXT,
    adult             BOOLEAN,
    source_lists      TEXT[],
    payload           JSONB NOT NULL,
    synced_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS genres (
    id   BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id BIGINT NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    genre_id BIGINT NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS people (
    id                   BIGINT PRIMARY KEY,
    name                 TEXT NOT NULL,
    known_for_department TEXT,
    gender               INT,
    popularity           DOUBLE PRECISION,
    profile_path         TEXT
);

CREATE TABLE IF NOT EXISTS movie_cast (
    credit_id  TEXT PRIMARY KEY,
    movie_id   BIGINT NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    person_id  BIGINT NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    character  TEXT,
    cast_order INT
);

CREATE INDEX IF NOT EXISTS idx_movie_genres_genre_id ON movie_genres (genre_id);
CREATE INDEX IF NOT EXISTS idx_movie_cast_movie_id   ON movie_cast (movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_cast_person_id  ON movie_cast (person_id);
"""

with lakebase.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

print("Tables ready: movies, genres, movie_genres, people, movie_cast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the genre reference table
# MAGIC
# MAGIC A single `/genre/movie/list` call maps every genre id to its name. We load
# MAGIC it first so `movie_genres` rows always have a genre to point at.

# COMMAND ----------

from psycopg2.extras import Json, execute_values

genres = client.get_genres()
genre_rows = [(g["id"], g["name"]) for g in genres]

with lakebase.get_connection() as conn:
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO genres (id, name) VALUES %s
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            genre_rows,
        )
    conn.commit()

print(f"Loaded {len(genre_rows)} genres.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect movie ids from the curated lists
# MAGIC
# MAGIC Walk `NUM_PAGES` pages of both `/movie/popular` and `/movie/top_rated`,
# MAGIC remembering which list(s) each movie id showed up in (the same title often
# MAGIC appears on both).

# COMMAND ----------

# movie_id -> set of source list names it appeared in
movie_sources: dict[int, set[str]] = {}

for list_name in SOURCE_LISTS:
    for page in range(1, NUM_PAGES + 1):
        results = client.get_movie_list(list_name, page=page)
        if not results:
            break  # ran past the last available page
        for movie in results:
            movie_sources.setdefault(movie["id"], set()).add(list_name)
    print(f"Collected ids after '{list_name}': {len(movie_sources)} distinct movies so far")

print(f"\nTotal distinct movies to ingest: {len(movie_sources)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch full details + credits for each movie
# MAGIC
# MAGIC `append_to_response=credits` folds the cast list into the same detail call,
# MAGIC so it's one request per movie. Any movie that fails to fetch is skipped
# MAGIC rather than failing the whole run.

# COMMAND ----------

movie_details: list[dict] = []
failed_ids: list[int] = []

for i, movie_id in enumerate(movie_sources, start=1):
    try:
        detail = client.get_movie_details(movie_id, append_to_response="credits")
        detail["_source_lists"] = sorted(movie_sources[movie_id])
        movie_details.append(detail)
    except Exception as exc:  # noqa: BLE001 - keep going on any single failure
        failed_ids.append(movie_id)
        print(f"  skip {movie_id}: {exc}")
    if i % 50 == 0:
        print(f"  fetched {i}/{len(movie_sources)} movies")

print(f"\nFetched details for {len(movie_details)} movies ({len(failed_ids)} failed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform + upsert into Lakebase
# MAGIC
# MAGIC Build the rows for each table from the fetched JSON, then upsert them all
# MAGIC inside one transaction. `ON CONFLICT` makes the whole notebook safely
# MAGIC re-runnable - running it again refreshes existing rows instead of
# MAGIC erroring. Insert order respects the foreign keys: movies + genres +
# MAGIC people before the join tables that reference them.

# COMMAND ----------


def clean_date(value: str | None) -> str | None:
    """TMDB returns '' for unknown release dates; Postgres DATE wants NULL."""
    return value or None


movie_rows = []
movie_genre_rows = []
people_by_id: dict[int, tuple] = {}
cast_rows = []

for d in movie_details:
    movie_rows.append(
        (
            d["id"],
            d.get("title") or d.get("original_title") or "",
            d.get("original_title"),
            d.get("overview"),
            d.get("tagline"),
            clean_date(d.get("release_date")),
            d.get("runtime"),
            d.get("status"),
            d.get("original_language"),
            d.get("popularity"),
            d.get("vote_average"),
            d.get("vote_count"),
            d.get("budget"),
            d.get("revenue"),
            d.get("poster_path"),
            d.get("backdrop_path"),
            d.get("adult"),
            d["_source_lists"],
            Json(d),
        )
    )

    for g in d.get("genres", []) or []:
        movie_genre_rows.append((d["id"], g["id"]))

    # Keep only the top-billed cast (lowest `order`), capped at CAST_LIMIT.
    cast = sorted(
        d.get("credits", {}).get("cast", []) or [],
        key=lambda c: c.get("order", 10_000),
    )[:CAST_LIMIT]
    for c in cast:
        people_by_id[c["id"]] = (
            c["id"],
            c.get("name") or "",
            c.get("known_for_department"),
            c.get("gender"),
            c.get("popularity"),
            c.get("profile_path"),
        )
        cast_rows.append(
            (
                c["credit_id"],
                d["id"],
                c["id"],
                c.get("character"),
                c.get("order"),
            )
        )

people_rows = list(people_by_id.values())

print(
    f"Prepared: {len(movie_rows)} movies, {len(movie_genre_rows)} movie-genre links, "
    f"{len(people_rows)} people, {len(cast_rows)} cast credits"
)

# COMMAND ----------

with lakebase.get_connection() as conn:
    with conn.cursor() as cur:
        # 1. movies
        execute_values(
            cur,
            """
            INSERT INTO movies (
                id, title, original_title, overview, tagline, release_date,
                runtime, status, original_language, popularity, vote_average,
                vote_count, budget, revenue, poster_path, backdrop_path, adult,
                source_lists, payload
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                title             = EXCLUDED.title,
                original_title    = EXCLUDED.original_title,
                overview          = EXCLUDED.overview,
                tagline           = EXCLUDED.tagline,
                release_date      = EXCLUDED.release_date,
                runtime           = EXCLUDED.runtime,
                status            = EXCLUDED.status,
                original_language = EXCLUDED.original_language,
                popularity        = EXCLUDED.popularity,
                vote_average      = EXCLUDED.vote_average,
                vote_count        = EXCLUDED.vote_count,
                budget            = EXCLUDED.budget,
                revenue           = EXCLUDED.revenue,
                poster_path       = EXCLUDED.poster_path,
                backdrop_path     = EXCLUDED.backdrop_path,
                adult             = EXCLUDED.adult,
                source_lists      = EXCLUDED.source_lists,
                payload           = EXCLUDED.payload,
                synced_at         = now()
            """,
            movie_rows,
            page_size=100,
        )

        # 2. people (must exist before movie_cast FK)
        execute_values(
            cur,
            """
            INSERT INTO people (id, name, known_for_department, gender, popularity, profile_path)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name                 = EXCLUDED.name,
                known_for_department = EXCLUDED.known_for_department,
                gender               = EXCLUDED.gender,
                popularity           = EXCLUDED.popularity,
                profile_path         = EXCLUDED.profile_path
            """,
            people_rows,
            page_size=100,
        )

        # 3. movie_genres join
        if movie_genre_rows:
            execute_values(
                cur,
                """
                INSERT INTO movie_genres (movie_id, genre_id) VALUES %s
                ON CONFLICT (movie_id, genre_id) DO NOTHING
                """,
                movie_genre_rows,
                page_size=200,
            )

        # 4. movie_cast join
        if cast_rows:
            execute_values(
                cur,
                """
                INSERT INTO movie_cast (credit_id, movie_id, person_id, character, cast_order)
                VALUES %s
                ON CONFLICT (credit_id) DO UPDATE SET
                    character  = EXCLUDED.character,
                    cast_order = EXCLUDED.cast_order
                """,
                cast_rows,
                page_size=200,
            )
    conn.commit()

print("Upsert complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Row counts per table, plus a peek at a few movies with their genres and
# MAGIC top cast, to confirm the joins line up.

# COMMAND ----------

for table in ("movies", "genres", "movie_genres", "people", "movie_cast"):
    rows = lakebase.run_query(f"SELECT count(*) AS n FROM {table}")
    print(f"{table:>13}: {rows[0]['n']}")

# COMMAND ----------

sample = lakebase.run_query(
    """
    SELECT
        m.title,
        m.release_date,
        m.vote_average,
        string_agg(DISTINCT g.name, ', ') AS genres,
        (SELECT string_agg(p.name, ', ')
           FROM (
             SELECT mc.person_id, mc.cast_order
             FROM movie_cast mc
             WHERE mc.movie_id = m.id
             ORDER BY mc.cast_order
             LIMIT 5
           ) top
           JOIN people p ON p.id = top.person_id) AS top_cast
    FROM movies m
    LEFT JOIN movie_genres mg ON mg.movie_id = m.id
    LEFT JOIN genres g        ON g.id = mg.genre_id
    GROUP BY m.id, m.title, m.release_date, m.vote_average
    ORDER BY m.popularity DESC
    LIMIT 10
    """
)

for r in sample:
    print(f"\n{r['title']} ({r['release_date']}) - {r['vote_average']}")
    print(f"  genres: {r['genres']}")
    print(f"  cast:   {r['top_cast']}")
