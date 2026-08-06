"""
Sample data for the AI Movie Night Planner, and the only place it is defined.

**Running this file deletes every existing group and reseeds.** That is its
whole job - no flags, no confirmation:

    python seed_data.py

`setup_database.py` imports `seed_if_empty()` instead, which only ever seeds a
database with no groups yet, so creating the schema never disturbs groups, chat
or ratings you've made.

Only movie-independent rows live here: users (including the real account,
pcast008@gmail.com, plus a few sample teammates), one sample group with its
members, and a little chat. Nominations, watchlist items and ratings all point at
a movie_id, so they are left to be created in the app once the TMDB catalog has
been ingested.
"""

import logging

from dotenv import load_dotenv

load_dotenv()

import lakebase  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("seed-data")


# The real account first, then sample teammates on example.com. app.py will also
# upsert a users row for whoever logs in, so this is a convenience for a fresh
# database, not the only way users appear.
OWNER_EMAIL = "pcast008@gmail.com"
SAMPLE_USERS = [
    OWNER_EMAIL,
    "alex@example.com",
    "sam@example.com",
    "priya@example.com",
    "marcus@example.com",
]

# One sample group so a fresh database has something to open: (name, created_by,
# [(member_email, role), ...], [(author_email, message_text), ...]). The owner
# also appears in the member list with role 'owner'.
SAMPLE_GROUP = {
    "name": "Friday Film Club",
    "created_by": OWNER_EMAIL,
    "members": [
        (OWNER_EMAIL, "owner"),
        ("alex@example.com", "member"),
        ("sam@example.com", "member"),
        ("priya@example.com", "member"),
    ],
    "messages": [
        (OWNER_EMAIL,
         "Welcome to Friday Film Club! Drop movie ideas here and we'll vote "
         "on what to watch this week."),
        ("alex@example.com",
         "I'm in for something sci-fi - been meaning to catch up on the genre."),
        ("priya@example.com",
         "Anything but a three-hour epic please, it's a school night."),
        ("sam@example.com",
         "Seconded. Let's nominate a few and put it to a vote."),
    ],
}


def _group_count() -> int:
    return lakebase.run_query("SELECT COUNT(*) AS count FROM groups")[0]["count"]


def insert_sample_data() -> None:
    """Insert the sample users, group, memberships and chat. Assumes no groups.

    One connection, one transaction: the group's generated id is needed for its
    members and messages, and a failure partway through shouldn't leave a group
    with half its members.
    """
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for email in SAMPLE_USERS:
                cur.execute(
                    "INSERT INTO users (email) VALUES (%s) "
                    "ON CONFLICT (email) DO NOTHING",
                    (email,),
                )

            cur.execute(
                """
                INSERT INTO groups (name, created_by)
                VALUES (
                    %s,
                    (SELECT user_id FROM users WHERE email = %s)
                )
                RETURNING group_id
                """,
                (SAMPLE_GROUP["name"], SAMPLE_GROUP["created_by"]),
            )
            group_id = cur.fetchone()["group_id"]

            for email, role in SAMPLE_GROUP["members"]:
                cur.execute(
                    """
                    INSERT INTO group_members (group_id, user_id, role)
                    VALUES (
                        %s,
                        (SELECT user_id FROM users WHERE email = %s),
                        %s
                    )
                    """,
                    (group_id, email, role),
                )

            for email, message_text in SAMPLE_GROUP["messages"]:
                cur.execute(
                    """
                    INSERT INTO group_messages (group_id, user_id, message_text)
                    VALUES (
                        %s,
                        (SELECT user_id FROM users WHERE email = %s),
                        %s
                    )
                    """,
                    (group_id, email, message_text),
                )
        conn.commit()

    logger.info(
        "seeded 1 group (%s) with %s members and %s messages",
        SAMPLE_GROUP["name"],
        len(SAMPLE_GROUP["members"]),
        len(SAMPLE_GROUP["messages"]),
    )


def seed_if_empty() -> None:
    """Seed only when there are no groups yet. This is what setup_database calls.

    Guarding on emptiness (rather than ON CONFLICT with hard-coded ids) keeps the
    BIGSERIAL sequences honest - hard-coding primary keys would leave a sequence
    pointing at 1 and the first app-created row would collide.
    """
    existing = _group_count()
    if existing:
        logger.info(
            "groups table already has %s row(s) - skipping sample data", existing
        )
        return
    insert_sample_data()


def replace_all() -> None:
    """Delete every group, then seed. Destructive, and only ever explicit.

    Memberships, chat, nominations, votes and watchlist items all go with their
    group through ON DELETE CASCADE. Users left referenced by nothing are cleared
    too, so stale sample teammates don't linger - anyone still referenced (by a
    group they created, a membership, a message, a nomination, a vote, a
    watchlist item or a rating) is untouched, and the app recreates a user row on
    their next login.
    """
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM groups")
            deleted = cur.rowcount
            cur.execute(
                """
                DELETE FROM users u
                WHERE NOT EXISTS (SELECT 1 FROM groups g
                                  WHERE g.created_by = u.user_id)
                  AND NOT EXISTS (SELECT 1 FROM group_members gm
                                  WHERE gm.user_id = u.user_id)
                  AND NOT EXISTS (SELECT 1 FROM group_messages m
                                  WHERE m.user_id = u.user_id)
                  AND NOT EXISTS (SELECT 1 FROM nominations n
                                  WHERE n.nominated_by = u.user_id)
                  AND NOT EXISTS (SELECT 1 FROM nomination_votes v
                                  WHERE v.user_id = u.user_id)
                  AND NOT EXISTS (SELECT 1 FROM watchlist_items w
                                  WHERE w.added_by = u.user_id)
                  AND NOT EXISTS (SELECT 1 FROM ratings r
                                  WHERE r.user_id = u.user_id)
                """
            )
        conn.commit()
    logger.info("deleted %s group(s) and their members, chat and poll data", deleted)
    insert_sample_data()


def verify_requirements() -> bool:
    """Check that the sample group came through: members and chat present."""
    groups = _group_count()
    thin_members = lakebase.run_query(
        """
        SELECT g.group_id, COUNT(gm.user_id) AS members
        FROM groups g
        LEFT JOIN group_members gm ON gm.group_id = g.group_id
        GROUP BY g.group_id
        HAVING COUNT(gm.user_id) < 2
        """
    )
    thin_chat = lakebase.run_query(
        """
        SELECT g.group_id, COUNT(m.message_id) AS messages
        FROM groups g
        LEFT JOIN group_messages m ON m.group_id = g.group_id
        GROUP BY g.group_id
        HAVING COUNT(m.message_id) < 1
        """
    )

    checks = [
        ("at least 1 group", groups >= 1, f"{groups} groups"),
        ("every group has 2+ members", not thin_members,
         "all groups have 2+" if not thin_members
         else f"{len(thin_members)} group(s) with fewer than 2"),
        ("every group has chat", not thin_chat,
         "all groups have messages" if not thin_chat
         else f"{len(thin_chat)} group(s) with no messages"),
    ]

    logger.info("")
    logger.info("Sample data checks:")
    for label, passed, detail in checks:
        logger.info("  [%s] %s - %s", "x" if passed else " ", label, detail)

    return all(passed for _, passed, _ in checks)


def summarize() -> None:
    rows = lakebase.run_query(
        """
        SELECT g.name,
               COUNT(DISTINCT gm.user_id)  AS members,
               COUNT(DISTINCT m.message_id) AS messages
        FROM groups g
        LEFT JOIN group_members gm ON gm.group_id = g.group_id
        LEFT JOIN group_messages m ON m.group_id = g.group_id
        GROUP BY g.group_id, g.name
        ORDER BY g.name
        """
    )
    logger.info("")
    logger.info("%-24s %8s %9s", "GROUP", "MEMBERS", "MESSAGES")
    for row in rows:
        logger.info("%-24s %8s %9s", row["name"], row["members"], row["messages"])


if __name__ == "__main__":
    # Running this file reseeds, full stop. No flags, so it behaves the same from
    # a shell, from %run, or from a notebook cell - none of which agree on what
    # sys.argv contains.
    replace_all()
    summarize()
    verify_requirements()
