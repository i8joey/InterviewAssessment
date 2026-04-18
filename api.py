import requests
import http.client
import pandas as pd
import json
import os
import kaggle

API_KEY = os.getenv("NBA_API_KEY")
BASE_URL = "v2.nba.api-sports.io"


def get_data(endpoint, filename=None):
    conn = http.client.HTTPSConnection(BASE_URL)

    headers = {'x-apisports-key': API_KEY}

    conn.request("GET", endpoint, headers=headers)

    res = conn.getresponse()

    data = res.read()
    data_json = json.loads(data.decode('utf-8'))

    df = pd.json_normalize(data_json['response'])

    if filename:
        df.to_parquet(filename, index=False)

    return df


def get_kaggle_data():
    kaggle.api.authenticate()
    kaggle.api.dataset_download_file("eoinamoore/historical-nba-data-and-player-box-scores", file_name="Games.csv",
                                     path="data/")


def fetch_teams():
    return get_data("/teams?league=standard", "data/teams.parquet")


def fetch_games():
    all_games = []

    # I was only able to pull one year at a time, so I used a loop with a list
    for season in range(2016, 2026):
        df = get_data(f"/games?league=standard&season={season}")
        df["season"] = season
        all_games.append(df)

    games_df = pd.concat(all_games, ignore_index=True)
    games_df.to_parquet("data/games.parquet", index=False)

    return games_df


def main():
    team_data = fetch_teams()
    games_df = fetch_games()
    get_kaggle_data()


if __name__ == "__main__":
    main()
