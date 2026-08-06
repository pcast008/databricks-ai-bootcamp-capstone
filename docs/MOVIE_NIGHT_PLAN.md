# AI Movie Night Planner — Master Build Plan

> Living build plan for the Databricks AI Bootcamp capstone (#1, AI Movie Night
> Planner). Start each work session here; check phases off as they land.

---

## Context

The repo `databricks-ai-bootcamp-capstone` (formerly the day-2 stock app) is being
rebuilt into **Capstone #1: AI Movie Night Planner**. We already have:

- `tmdb_client.py` — TMDB API client (secret-scoped Bearer auth; `get_genres`,
  `get_movie_list`, `get_movie_details`).
- `notebooks/ingest_tmdb_movies.py` — ingestion pipeline that creates + fills the
  TMDB-backed tables (`movies`, `genres`, `movie_genres`, `people`, `movie_cast`)
  via `lakebase.py` + psycopg2.
- `CAPSTONE_REQUIREMENTS.md`, `setup_tmdb_secret.py`.

We are **leveraging the sibling app** at
`C:\Users\pcast\Documents\databricks-lakebase-app-ticketing-system` as the template.
That app is a mature Databricks App: pooled `lakebase.py`, a clean JSON-API `app.py`
(identity via `X-Forwarded-Email`, `ApiError` handling, lookup tables, server-side
validation, ownership `403`s), `setup_database.py` + `seed_data.py` (idempotent DDL,
seed-if-empty, migrations, verify/summarize), and a single-page UI
(`templates/index.html` + `static/css/app.css` + `static/js/app.js` + fonts).

**Goal:** meet or exceed *every* capstone requirement and *every* AI Movie Night
Planner capability — no exceptions — built in documented phases, one section at a
time. Near-term phases build the app minus AI; later phases add the Spark embeddings
pipeline and the agent so the full requirement set is satisfied.

**Ticket → Movie domain mapping:** `ticket` → `group`, `ticket_messages` → group
chat, list+detail UI → groups + movie catalog. The ticketing app is the proven
skeleton; the movie domain is the new flesh.

---

## Requirement coverage matrix (no exceptions)

### Capstone general requirements

| Requirement | Where it's met | Phase |
|---|---|---|
| A data pipeline in **Spark** | **Embeddings pipeline** (distributed `mapInPandas`, mirrors the existing `ingest_ticker_news_embeddings` notebook). *(TMDB ingestion is psycopg2 — Spark requirement is carried by embeddings.)* | P6 |
| Integration with ≥1 third-party API | TMDB — batch ingest **and** live `search` / `watch-providers` in the app | P3, P5c |
| Processing of unstructured data | Plot summaries (+ keywords, cast) embedded to pgvector | P6 |
| A Databricks App with a frontend | The movie-night app | P4–P5 |
| An AI agent that does stuff (read+write tools) | Agent phase: semantic search, compare, add-to-watchlist, record rating, avoid watched/disliked, recommend | P7 |

### AI Movie Night Planner specifics

| Capability | Where it's met | Phase |
|---|---|---|
| Lakebase tables (users, groups, group_members, movies, ratings, watchlist_items, recommendations) | All created; `recommendations` in agent phase | P1, P3, P7 |
| Embed plot/keywords/cast; semantic retrieval | Embeddings + agent | P6, P7 |
| Agent: search & explain recommendations | Agent | P7 |
| Agent: compare movies | Agent | P7 |
| Agent: add to group watchlist | Manual in app (P5d), automated by agent (P7) |
| Agent: record ratings after watching | Manual in app (P5e), automated by agent (P7) |
| Agent: avoid movies already watched/disliked | Data tracked + surfaced manually (P5e), enforced by agent (P7) |

**Exceeds baseline:** group **poll/voting** flow (nominate → vote → promote),
group **chat**, live **where-to-watch**, and a manual "avoid watched/disliked"
toggle that pre-implements the agent's avoidance rule.

---

## Architecture & reuse

**Copy/adapt from ticketing (into this repo, not overwriting our pipeline files):**

| From ticketing | Action |
|---|---|
| `lakebase.py` (pooled `ThreadedConnectionPool`) | **Replace** our simpler `lakebase.py`. Same `get_connection/run_query/run_write` interface the notebook already uses; our unused `get_engine`/sqlalchemy is dropped. |
| `app.py` patterns | Rewrite for movie domain: `ApiError` + JSON error handlers, `_current_user_*`, `_body`/`_required_text`/`_one_of` validators, ownership checks. |
| `setup_database.py`, `seed_data.py` | New movie-domain versions (idempotent DDL, seed-if-empty, verify, summarize). |
| `static/fonts/*` | Copy verbatim. |
| `static/css/app.css` | Reuse as the design system; adapt components. |
| `templates/index.html`, `static/js/app.js` | Fresh movie UI on the same structure (modals, drawer, `textContent`-safe rendering). |
| `requirements.txt`, `app.yaml`, `databricks.yml` | Adapt (`psycopg2-binary` + flask + sdk + dotenv). |

**Untouched:** `tmdb_client.py`, `notebooks/`, `sql/`, `CAPSTONE_REQUIREMENTS.md`.
**Replaced:** `app.py` (old stock app), `templates/index.html` (old stock UI).

---

## Final data model

**App-generated tables (created by `setup_database.py`):**

```
users            user_id BIGSERIAL PK · email TEXT UNIQUE · created_at
groups           group_id BIGSERIAL PK · name TEXT · created_by → users · created_at
group_members    (group_id → groups ON DELETE CASCADE, user_id → users) PK · role TEXT('owner'|'member') · added_at
group_messages   message_id BIGSERIAL PK · group_id → groups CASCADE · user_id → users · message_text · created_at · edited_at
nominations      nomination_id BIGSERIAL PK · group_id → groups CASCADE · movie_id BIGINT(soft) · nominated_by → users · created_at · UNIQUE(group_id, movie_id)
nomination_votes (nomination_id → nominations CASCADE, user_id → users) PK · created_at        -- approval voting
watchlist_items  watchlist_item_id BIGSERIAL PK · group_id → groups CASCADE · movie_id BIGINT(soft) · added_by → users · status TEXT('candidate'|'watched') · added_at · watched_at · UNIQUE(group_id, movie_id)
ratings          rating_id BIGSERIAL PK · user_id → users · movie_id BIGINT(soft) · reaction TEXT('up'|'down') NULL · created_at · updated_at · UNIQUE(user_id, movie_id)
```

- **`movie_id` is a soft reference** (plain `BIGINT`, no FK to the pipeline-owned
  `movies` table) so `setup_database.py` runs independently of ingestion. App-layer
  writes still guarantee valid ids (you can only act on a movie found in the
  catalog). Can be hardened to real FKs later once `movies` exists.
- **`ratings`**: a row existing = the user **watched** it; `reaction='down'` =
  **disliked**. Powers "avoid watched or disliked" via one join to `group_members`.
  Group score = "👍 N of M who've seen it" over that group's members.

**Movie/reference tables (owned by `notebooks/ingest_tmdb_movies.py`, for reference):**
`movies`, `genres`, `movie_genres`, `people`, `movie_cast`.

**Agent phase adds:** `recommendations` and a `movie_embeddings` (pgvector) table.

---

## API surface (target, built incrementally)

- `GET /`, `GET /healthz`
- **Groups:** `GET /api/groups` (mine) · `POST /api/groups` · `GET /api/groups/<id>` ·
  `PATCH /api/groups/<id>` · `DELETE /api/groups/<id>` ·
  `POST /api/groups/<id>/members` · `DELETE /api/groups/<id>/members/<user_id>`
- **Chat:** `GET|POST /api/groups/<id>/messages` · `PATCH|DELETE /api/messages/<id>`
- **Catalog:** `GET /api/movies` (q/genre/sort/paginate, group-aware badges) ·
  `GET /api/movies/<id>` · `GET /api/movies/<id>/providers` (live TMDB)
- **Poll:** `GET|POST /api/groups/<id>/nominations` · `DELETE /api/nominations/<id>` ·
  `POST|DELETE /api/nominations/<id>/votes` · `POST /api/nominations/<id>/promote`
- **Watchlist:** `GET|POST /api/groups/<id>/watchlist` ·
  `PATCH /api/watchlist/<id>` (mark watched) · `DELETE /api/watchlist/<id>`
- **Ratings:** `PUT /api/movies/<id>/rating` (`up`|`down`|`seen`|`clear`)

Ownership rules: you only see groups you belong to; only members chat / nominate /
vote / edit a group's watchlist; ratings and your own messages are yours to edit.
Enforced server-side with `403` (UI hides controls as presentation only).

---

## Phases

Each phase is independently runnable/verifiable. Build order top to bottom.

### P0 — Foundations
- Replace `lakebase.py` with the pooled version; update `requirements.txt`,
  `app.yaml`, `.env.example`, `databricks.yml` for the movie app.
- **Done when:** `python -c "import lakebase"` works; deps install.

### P1 — Schema
- New `setup_database.py`: idempotent DDL for the 8 app tables above; a
  `create_tables()` + `create_indexes()` + (later) `seed` entrypoint.
- **Done when:** running it creates all 8 tables; re-running is a no-op.

### P2 — Seed data (non-API only)
- New `seed_data.py`: seed `users` incl. **pcast008@gmail.com** + a few
  `example.com` sample teammates; one sample `group` ("Friday Film Club") with
  members and a couple of `group_messages`. **No** `nominations`/`watchlist`/
  `ratings` (they need movies). `seed_if_empty()` + destructive `replace_all()` +
  `verify`/`summarize`, mirroring ticketing.
- **Done when:** a fresh DB shows the sample group with members + chat; re-run safe.

### P3 — Movie data pipeline
- Run the existing `notebooks/ingest_tmdb_movies.py` to populate the catalog.
- Add live-lookup methods to `tmdb_client.py`: `search_movie(query)`,
  `get_watch_providers(movie_id)`, `get_videos(movie_id)` (trailers).
- **Done when:** `movies`/`genres`/`cast` populated; new client methods return data.

### P4 — App shell
- `app.py` core: Flask, identity (`_current_user_id` upserting `users`), error
  handlers, `/`, `/healthz`. `templates/index.html` shell + `static/css/app.css`
  (from ticketing) + `static/js/app.js` base (fetch helper, banner, modal plumbing).
- **Done when:** app boots locally, renders the shell, health check passes.

### P5 — App features (one section at a time)
- **P5a Groups & members** — list your groups, create, add/remove members, group hub view.
- **P5b Catalog & detail** — browse/search/filter `movies` from Lakebase; movie
  detail (poster, plot, genres, cast); **live where-to-watch**.
- **P5c Poll** — nominate from catalog, approval-vote, **promote → watchlist**.
- **P5d Watchlist** — add, mark watched, remove; group's shared list.
- **P5e Ratings & avoidance** — 👍/👎/"Seen it"; group averages; **"hide watched or
  disliked by members"** toggle + badges (manual version of the agent rule).
- **P5f Chat** — group message thread (reuse ticket-messages behavior).
- **Done when:** each section works end-to-end against real Lakebase.

### P6 — Embeddings pipeline (Spark) ⟵ satisfies the Spark requirement
- New notebook: read `movies.overview` (+ keywords/cast) from Lakebase, embed with
  a sentence-transformers model via **Spark `mapInPandas`**, write vectors to a
  `movie_embeddings` pgvector table. Model dims handled like the news notebook.
- **Done when:** `movie_embeddings` populated; a cosine query returns sensible neighbors.

### P7 — AI agent
- `recommendations` table; agent with read tools (semantic search over embeddings,
  compare movies, list group ratings/history) and **write** tools (add to watchlist,
  record rating, save recommendation). Enforces **avoid watched/disliked**. Surfaced
  in the app (a "Ask the planner" panel in the group hub).
- **Done when:** agent recommends from the catalog, explains why, writes to Lakebase,
  and never suggests a movie a member watched or disliked.

### P8 — Polish, docs, deploy
- README rewrite (movie domain), requirement-coverage checklist, screenshots, deploy
  verification on Databricks Apps.

---

## Verification

- **Per phase:** the "Done when" line above.
- **Static (each code change):** `python -c "import ast; ast.parse(open(f).read())"`
  for touched Python; `python -c "import lakebase, app"` import check.
- **Local run:** `python setup_database.py` then `python app.py` → exercise each
  feature in the browser against the real Lakebase instance (local and deployed
  share one DB).
- **End-to-end capstone check (final):** walk the matrix above and confirm each row
  demonstrably works in the deployed app; confirm the Spark embeddings job runs and
  the agent honors the avoid-rule.

---

## Locked decisions (from planning session)

- Replace `app.py`; keep `tmdb_client.py`/notebook/pipeline intact.
- Ratings = **thumbs up/down + "Seen it"**, **global per user** (unique
  `user_id,movie_id`); group shows the average.
- Poll = **approval voting** (upvote any) + **manual promote** to watchlist.
- Movie lifecycle in a group: `catalog → nominated → voted → watchlist → watched`.
- `setup_database.py` + `seed_data.py` pattern — **no** `tmdb_sql/` folder.
- `movie_id` soft references (no hard FK to `movies`) so setup is pipeline-independent.
- Seed users now (incl. pcast008@gmail.com) + sample group/members/chat; no
  movie-dependent rows until the catalog is ingested.

---

## Progress log

- [x] P0 — Foundations
- [x] P1 — Schema
- [x] P2 — Seed data
- [x] P3 — Movie data pipeline
- [ ] P4 — App shell
- [ ] P5a Groups & members
- [ ] P5b Catalog & detail
- [ ] P5c Poll
- [ ] P5d Watchlist
- [ ] P5e Ratings & avoidance
- [ ] P5f Chat
- [ ] P6 — Embeddings pipeline (Spark)
- [ ] P7 — AI agent
- [ ] P8 — Polish, docs, deploy
