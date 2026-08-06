"""
Client for the TMDB (The Movie Database) API.

The API credential is stored in a Databricks secret scope and resolved at
runtime via the Databricks SDK - it is never stored in code, env files, or
app.yaml.

TMDB issues two credentials on your API settings page. This client uses the
**API Read Access Token** (v4 auth - the long `eyJ...` JWT), passed as a Bearer
header, which works against the v3 REST endpoints used here.
"""

import base64
import os
import time
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()

_SCOPE = os.environ.get("TMDB_SECRET_SCOPE", "tmdb")
_KEY = os.environ.get("TMDB_SECRET_KEY", "api-key")
_BASE_URL = os.environ.get("TMDB_API_BASE_URL", "https://api.themoviedb.org/3")

_DEFAULT_TIMEOUT = 30
_DEFAULT_LANGUAGE = "en-US"


def _get_read_access_token(scope: str = _SCOPE, key: str = _KEY) -> str:
    """Fetch and decode the TMDB read access token from the Databricks secret scope."""
    secret = _w.secrets.get_secret(scope=scope, key=key)
    return base64.b64decode(secret.value).decode("utf-8")


class TmdbClient:
    """Thin wrapper around the TMDB v3 REST API with Bearer auth + optional throttle."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: int = _DEFAULT_TIMEOUT,
        language: str = _DEFAULT_LANGUAGE,
        requests_per_second: float | None = None,
        secret_scope: str = _SCOPE,
        secret_key: str = _KEY,
    ):
        self.base_url = (base_url or _BASE_URL).rstrip("/")
        self.timeout = timeout
        self.language = language
        # Client-side throttle: minimum seconds between requests. TMDB's limits
        # are generous, but this keeps scripted bulk pulls polite.
        self._min_interval = 1.0 / requests_per_second if requests_per_second else 0.0
        self._last_call_at = 0.0
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {_get_read_access_token(secret_scope, secret_key)}",
                "Accept": "application/json",
            }
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if self._min_interval:
            wait = self._min_interval - (time.monotonic() - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
        query: dict[str, Any] = {"language": self.language}
        if params:
            query.update(params)
        resp = self._session.get(f"{self.base_url}{path}", params=query, timeout=self.timeout)
        self._last_call_at = time.monotonic()
        resp.raise_for_status()
        return resp.json()

    def get_genres(self) -> list[dict]:
        """GET /genre/movie/list - every genre id -> name."""
        return self.get("/genre/movie/list").get("genres", [])

    def get_movie_list(self, list_name: str, page: int = 1) -> list[dict]:
        """
        GET a curated list page (e.g. list_name="popular" or "top_rated").
        Returns just the "results" array (20 movies per page).
        """
        data = self.get(f"/movie/{list_name}", params={"page": page})
        return data.get("results", [])

    def get_movie_details(self, movie_id: int, append_to_response: str = "credits") -> dict:
        """
        GET /movie/{id} full details. `append_to_response` folds extra
        sub-resources (default "credits" -> cast/crew) into the same request,
        so cast comes back in one call instead of two.
        """
        return self.get(
            f"/movie/{movie_id}",
            params={"append_to_response": append_to_response},
        )

    def search_movie(self, query: str, page: int = 1) -> list[dict]:
        """
        GET /search/movie - live title search. Returns the "results" array
        (20 matches per page). Used by the app to find movies to nominate that
        may not be in the ingested catalog.
        """
        if not query or not query.strip():
            return []
        data = self.get("/search/movie", params={"query": query.strip(), "page": page})
        return data.get("results", [])

    def get_watch_providers(self, movie_id: int, region: str | None = None) -> dict:
        """
        GET /movie/{id}/watch/providers - where-to-watch data, powered by
        JustWatch. TMDB returns a "results" map keyed by ISO-3166 country code;
        each entry has optional flatrate/rent/buy provider arrays plus a
        JustWatch "link". Pass `region` (e.g. "US") to return just that country,
        else the full map.
        """
        results = self.get(f"/movie/{movie_id}/watch/providers").get("results", {})
        if region:
            return results.get(region.upper(), {})
        return results

    def get_videos(self, movie_id: int) -> list[dict]:
        """
        GET /movie/{id}/videos - trailers, teasers, clips. Returns the "results"
        array; each item has site/type/key (e.g. a YouTube key) for embedding.
        """
        return self.get(f"/movie/{movie_id}/videos").get("results", [])
