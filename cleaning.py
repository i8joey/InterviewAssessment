import pandas as pd
from pandas import read_parquet, read_csv


def clean_teams(df):
    df = df[(df["nbaFranchise"] == True)]

    df = df[[
        "id",
        "name",
        "leagues.standard.conference",
        "leagues.standard.division"]]

    df = df.rename(columns={
        "id": "team_id",
        "name": "team_name",
        "leagues.standard.conference": "conference",
        "leagues.standard.division": "division"})
    df = df[~df["team_name"].str.contains(r"\bteam\b", case=False, na=False)]

    return df


def clean_games(df, valid_teams):
    df = df[[
        "id",
        "season",
        "date.start",
        "teams.home.name",
        "teams.visitors.name",
        "scores.home.points",
        "scores.visitors.points"]]

    df = df.rename(columns={
        "id": "game_id",
        "season": "season",
        "date.start": "date",
        "teams.home.name": "home_team",
        "teams.visitors.name": "away_team",
        "scores.home.points": "home_score",
        "scores.visitors.points": "away_score"})

    # Keep date value consistent across tables
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None).dt.normalize()

    # Ensure all games have scores
    df = df[(df["home_score"] > 0) & (df["away_score"] > 0)]

    # Ensure all teams are in the NBA
    df = df[df["home_team"].isin(valid_teams) & df["away_team"].isin(valid_teams)]

    return df

# Finding start/end of data from API Sports
def get_dates(parquet_file_path):
    df = pd.read_parquet(parquet_file_path)

    df["date.start"] = pd.to_datetime(df["date.start"], utc=True).dt.tz_localize(None).dt.normalize()

    return df["date.start"].min(), df["date.start"].max()


def clean_kaggle_games(df, valid_teams, parquet_file_path="data/games.parquet"):
    first_date, last_date = get_dates(parquet_file_path)

    df = df[[
        "gameId",
        "gameDateTimeEst",
        "hometeamName",
        "awayteamName",
        "homeScore",
        "awayScore"]]

    df = df.rename(columns={
        "gameId": "game_id",
        "gameDateTimeEst": "date",
        "homeScore": "home_score",
        "awayScore": "away_score"})

    team_name_map = {
        "Hawks": "Atlanta Hawks",
        "Celtics": "Boston Celtics",
        "Nets": "Brooklyn Nets",
        "Hornets": "Charlotte Hornets",
        "Bulls": "Chicago Bulls",
        "Cavaliers": "Cleveland Cavaliers",
        "Mavericks": "Dallas Mavericks",
        "Nuggets": "Denver Nuggets",
        "Pistons": "Detroit Pistons",
        "Warriors": "Golden State Warriors",
        "Rockets": "Houston Rockets",
        "Pacers": "Indiana Pacers",
        "Clippers": "LA Clippers",
        "Lakers": "Los Angeles Lakers",
        "Grizzlies": "Memphis Grizzlies",
        "Heat": "Miami Heat",
        "Bucks": "Milwaukee Bucks",
        "Timberwolves": "Minnesota Timberwolves",
        "Pelicans": "New Orleans Pelicans",
        "Knicks": "New York Knicks",
        "Thunder": "Oklahoma City Thunder",
        "Magic": "Orlando Magic",
        "76ers": "Philadelphia 76ers",
        "Suns": "Phoenix Suns",
        "Trail Blazers": "Portland Trail Blazers",
        "Kings": "Sacramento Kings",
        "Spurs": "San Antonio Spurs",
        "Raptors": "Toronto Raptors",
        "Jazz": "Utah Jazz",
        "Wizards": "Washington Wizards"}

    df["home_team"] = df["hometeamName"].map(team_name_map)
    df["away_team"] = df["awayteamName"].map(team_name_map)

    # Changing type from STR to INT
    df["game_id"] = df["game_id"].astype(int)
    df["home_score"] = df["home_score"].astype(int)
    df["away_score"] = df["away_score"].astype(int)

    # Keep date value consistent across tables
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None).dt.normalize()

    # Adding season year
    df["season"] = df["date"].apply(lambda x: x.year - 1 if x.month < 9 else x.year)

    # Filling in missing data from API Sports
    df = df[(df["date"] < first_date) | (df["date"] > last_date)]
    df = df[df["date"] >= "2016-01-01"]

    # Ensure all games have scores
    df = df[(df["home_score"] > 0) & (df["away_score"] > 0)]

    df = df[df["home_team"].isin(valid_teams) & df["away_team"].isin(valid_teams)]

    return df[["game_id", "season", "date", "home_team", "away_team", "home_score", "away_score"]]


def combine_games(games1, games2):
    combined = pd.concat([games1, games2], ignore_index=True)
    combined = combined.drop_duplicates(subset=["game_id"])
    combined = combined.sort_values("date").reset_index(drop=True)

    # Removing All Star games and All Star teams (eg. Team Lebron)
    combined = combined[
        ~combined["home_team"].str.contains("All Stars", na=False) &
        ~combined["away_team"].str.contains("All Stars", na=False) &
        ~combined["home_team"].str.contains("Team", na=False) &
        ~combined["away_team"].str.contains("Team", na=False)]

    return combined


def main():
    cleaned_teams = clean_teams(read_parquet("data/teams.parquet"))
    cleaned_teams.to_parquet("data/cleaned_teams.parquet", index=False)

    valid_teams = set(cleaned_teams["team_name"])

    cleaned_games = clean_games(read_parquet("data/games.parquet"), valid_teams)
    cleaned_games.to_parquet("data/cleaned_games.parquet", index=False)

    cleaned_kaggle_games = clean_kaggle_games(read_csv("data/Games.csv", low_memory=False), valid_teams)
    cleaned_kaggle_games.to_parquet("data/cleaned_kaggle_games.parquet", index=False)

    cleaned_combined_games = combine_games(cleaned_games, cleaned_kaggle_games)
    cleaned_combined_games.to_parquet("data/cleaned_combined_games.parquet", index=False)


if __name__ == "__main__":
    main()
