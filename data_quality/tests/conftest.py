"""Shared fixtures for Data Quality isolated test suite."""
from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def clean_tracks_sample() -> pd.DataFrame:
    return pd.DataFrame({
        "track_id": ["0123456789012345678901", "abcdefghij0123456789ab"],
        "track_name": ["Valid Song 1", "Valid Song 2"],
        "album_name": ["Album A", "Album B"],
        "artists": ["Artist A", "Artist B"],
        "popularity": [70, 45],
        "duration_ms": [210000, 185000],
        "explicit": [False, True],
        "danceability": [0.65, 0.82],
        "energy": [0.78, 0.45],
        "key": [5, 11],
        "loudness": [-5.4, -8.2],
        "mode": [1, 0],
        "speechiness": [0.04, 0.12],
        "acousticness": [0.15, 0.65],
        "instrumentalness": [0.0, 0.05],
        "liveness": [0.10, 0.22],
        "valence": [0.55, 0.38],
        "tempo": [124.0, 95.5],
        "time_signature": [4, 4],
        "genres": [["pop"], ["dance"]],
        "primary_genre": ["pop", "dance"],
        "genres_count": [1, 1],
    })


@pytest.fixture
def clean_charts_sample() -> pd.DataFrame:
    return pd.DataFrame({
        "chart_title": ["Song A", "Song B"],
        "chart_rank": [1, 200],
        "chart_date": ["2018-05-12", "2018-05-12"],
        "chart_artist": ["Artist A", "Artist B"],
        "track_url": ["https://open.spotify.com/track/0123456789012345678901", "https://open.spotify.com/track/0123456789012345678902"],
        "region": ["United States", "Global"],
        "chart": ["top200", "top200"],
        "trend": ["SAME_POSITION", "NEW_ENTRY"],
        "streams": [500000, 120000],
        "track_id": ["0123456789012345678901", "0123456789012345678902"],
        "has_streams": [True, True],
    })
