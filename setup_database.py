"""
One-time database setup: creates the AI Movie Night Planner schema in Lakebase
and loads sample data.

Run this once, manually, before starting the app:

    python setup_database.py

Safe to re-run. All DDL uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT
EXISTS, and the sample data is only inserted when there are no groups yet - so a
second run against a live database leaves your real groups, chat and ratings
alone.

This file owns the eight *app* tables (users, groups, group_members,
group_messages, nominations, nomination_votes, watchlist_items, ratings). The
movie *catalog* tables (movies, genres, movie_genres, people, movie_cast) are
owned by notebooks/ingest_tmdb_movies.py and are not touched here. movie_id is a
plain BIGINT soft reference (no FK to movies) so this setup runs independently of
catalog ingestion; the app layer only ever writes ids it found in the catalog.

Needs LAKEBASE_URL in your environment (see .env.example) or, failing that,
Databricks workspace auth so lakebase.py can read the connection URL from the
secret scope.
"""

import logging

from dotenv import load_dotenv

load_dotenv()

import lakebase  # noqa: E402
import seed_data  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("setup-database")


# ------------------------------------------------------------- table DDL
#
# Fresh schema: no lookup tables and no text->id migrations. The only enum-like
# columns (group_members.role, watchlist_items.status, ratings.reaction) are
# small, internal, and not user-facing sorted dropdowns, so a CHECK constraint is
# the right guarantee here rather than a lookup table + rank/label. Tables are
# created in foreign-key dependency order.

# The identity table. app.py upserts a row here the first time it sees an email
# (from X-Forwarded-Email), so every author reference resolves to a real user.
CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    user_id    BIGSERIAL   PRIMARY KEY,
    email      TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_GROUPS = """
CREATE TABLE IF NOT EXISTS groups (
    group_id   BIGSERIAL   PRIMARY KEY,
    name       TEXT        NOT NULL,
    created_by BIGINT      NOT NULL REFERENCES users(user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# The membership join table doubles as the access-control list: you only see a
# group if you have a row here. ON DELETE CASCADE on group_id means deleting a
# group takes its memberships with it. role is 'owner' or 'member'.
CREATE_GROUP_MEMBERS = """
CREATE TABLE IF NOT EXISTS group_members (
    group_id BIGINT      NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    user_id  BIGINT      NOT NULL REFERENCES users(user_id),
    role     TEXT        NOT NULL DEFAULT 'member'
                         CHECK (role IN ('owner', 'member')),
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
)
"""

# Group chat. edited_at NULL until the author edits the message.
CREATE_GROUP_MESSAGES = """
CREATE TABLE IF NOT EXISTS group_messages (
    message_id   BIGSERIAL   PRIMARY KEY,
    group_id     BIGINT      NOT NULL
                             REFERENCES groups(group_id) ON DELETE CASCADE,
    user_id      BIGINT      NOT NULL REFERENCES users(user_id),
    message_text TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    edited_at    TIMESTAMPTZ
)
"""

# Poll: a nomination is a movie put forward for a group. UNIQUE(group_id,
# movie_id) means a movie can only be nominated once per group; a second attempt
# is a no-op the app can turn into "already on the ballot".
CREATE_NOMINATIONS = """
CREATE TABLE IF NOT EXISTS nominations (
    nomination_id BIGSERIAL   PRIMARY KEY,
    group_id      BIGINT      NOT NULL
                              REFERENCES groups(group_id) ON DELETE CASCADE,
    movie_id      BIGINT      NOT NULL,
    nominated_by  BIGINT      NOT NULL REFERENCES users(user_id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (group_id, movie_id)
)
"""

# Approval voting: one row = one member upvoting one nomination. The PK enforces
# one vote per member per nomination; removing a vote deletes the row.
CREATE_NOMINATION_VOTES = """
CREATE TABLE IF NOT EXISTS nomination_votes (
    nomination_id BIGINT      NOT NULL
                              REFERENCES nominations(nomination_id)
                              ON DELETE CASCADE,
    user_id       BIGINT      NOT NULL REFERENCES users(user_id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (nomination_id, user_id)
)
"""

# The group's shared list. status is 'candidate' until the group watches it, then
# 'watched' (with watched_at set). UNIQUE(group_id, movie_id) keeps one row per
# movie per group, so promoting a nomination that's already listed is a no-op.
CREATE_WATCHLIST_ITEMS = """
CREATE TABLE IF NOT EXISTS watchlist_items (
    watchlist_item_id BIGSERIAL   PRIMARY KEY,
    group_id          BIGINT      NOT NULL
                                  REFERENCES groups(group_id) ON DELETE CASCADE,
    movie_id          BIGINT      NOT NULL,
    added_by          BIGINT      NOT NULL REFERENCES users(user_id),
    status            TEXT        NOT NULL DEFAULT 'candidate'
                                  CHECK (status IN ('candidate', 'watched')),
    added_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    watched_at        TIMESTAMPTZ,
    UNIQUE (group_id, movie_id)
)
"""

# Ratings are GLOBAL per user (UNIQUE(user_id, movie_id)), not per group: a row
# existing means the user has *watched* the movie; reaction 'down' means they
# disliked it, 'up' liked it, NULL is "seen it, no opinion". This is what powers
# "avoid movies a group member has already watched or disliked" - one join from a
# group's members to this table. A group's score for a movie is the up-count over
# the members who have a row for it.
CREATE_RATINGS = """
CREATE TABLE IF NOT EXISTS ratings (
    rating_id  BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users(user_id),
    movie_id   BIGINT      NOT NULL,
    reaction   TEXT        CHECK (reaction IN ('up', 'down')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, movie_id)
)
"""

# Index every foreign key that a query filters or joins on. Columns that already
# lead a primary key or unique constraint (group_members.group_id,
# nomination_votes.nomination_id, ratings.user_id) are already indexed by that
# constraint and are not repeated here.
CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_groups_created_by"
    "    ON groups (created_by)",
    # "which groups am I in" - user_id is the trailing PK column, so it needs its
    # own index.
    "CREATE INDEX IF NOT EXISTS idx_group_members_user_id"
    "    ON group_members (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_group_messages_group_id"
    "    ON group_messages (group_id)",
    "CREATE INDEX IF NOT EXISTS idx_group_messages_user_id"
    "    ON group_messages (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_nominations_group_id"
    "    ON nominations (group_id)",
    "CREATE INDEX IF NOT EXISTS idx_nomination_votes_user_id"
    "    ON nomination_votes (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_group_id"
    "    ON watchlist_items (group_id)",
    # Group-level lookups start from movie_id ("who in this group has seen X").
    "CREATE INDEX IF NOT EXISTS idx_watchlist_items_movie_id"
    "    ON watchlist_items (movie_id)",
    "CREATE INDEX IF NOT EXISTS idx_ratings_movie_id"
    "    ON ratings (movie_id)",
]


def create_tables() -> None:
    """Create the eight app tables in FK-dependency order. Idempotent."""
    for label, ddl in (
        ("users", CREATE_USERS),
        ("groups", CREATE_GROUPS),
        ("group_members", CREATE_GROUP_MEMBERS),
        ("group_messages", CREATE_GROUP_MESSAGES),
        ("nominations", CREATE_NOMINATIONS),
        ("nomination_votes", CREATE_NOMINATION_VOTES),
        ("watchlist_items", CREATE_WATCHLIST_ITEMS),
        ("ratings", CREATE_RATINGS),
    ):
        lakebase.run_write(ddl)
        logger.info("%s table ready", label)


def create_indexes() -> None:
    """Index the foreign keys queries filter and join on. Idempotent."""
    for ddl in CREATE_INDEXES:
        lakebase.run_write(ddl)
    logger.info("indexes ready")


if __name__ == "__main__":
    create_tables()
    create_indexes()
    # Sample data and the summary live in seed_data.py, so there's one copy of
    # them and it can be re-run on its own.
    seed_data.seed_if_empty()
    seed_data.summarize()
