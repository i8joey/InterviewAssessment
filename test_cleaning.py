import pandas as pd
import pytest
from cleaning import clean_teams, clean_games, clean_kaggle_games, combine_games


def test_clean_teams():
    df = pd.read_parquet("data/cleaned_teams.parquet")
    assert "team_name" in df.columns
    assert "conference" in df.columns
    assert df["team_name"].isnull().sum() == 0
    assert len(df) == 30  # exactly 30 NBA teams


def test_clean_games():
    df = pd.read_parquet("data/cleaned_games.parquet")
    assert df.shape[1] == 7
    assert df.isnull().sum().sum() == 0
    assert df["home_score"].dtype in ["int64", "float64"]
    assert df["away_score"].dtype in ["int64", "float64"]


def test_combine_games():
    df = pd.read_parquet("data/cleaned_combined_games.parquet")
    assert df["game_id"].duplicated().sum() == 0  # no duplicate games
    assert df["date"].min() >= pd.Timestamp("2016-01-01")
    assert df["date"].max() <= pd.Timestamp.today()
    assert df.isnull().sum().sum() == 0
    assert len(df["home_team"].unique()) == 30  # all 30 teams present


def test_no_allstar_games():
    df = pd.read_parquet("data/cleaned_combined_games.parquet")
    assert not df["home_team"].str.contains("All Stars", na=False).any()
    assert not df["away_team"].str.contains("All Stars", na=False).any()


def test_scores_are_positive():
    df = pd.read_parquet("data/cleaned_combined_games.parquet")
    assert (df["home_score"] > 0).all()
    assert (df["away_score"] > 0).all()
