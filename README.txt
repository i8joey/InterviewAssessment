# NBA Analysis
Analysis of NBA games from the last decade using data from [API Sports](https://api-sports.io/)
and [Kaggle](https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores).

## Setup
pip install -r requirements.txt

## Data Collection
python api.py

## Data Cleaning
python cleaning.py

## Run
streamlit run duckdb_setup.py

## Built With
Python 3.11, Pandas, DuckDB, Streamlit, Plotly

## Environment Variables
The following environment variables must be set before running:
`NBA_API_KEY` - API Sports API key

Set in PowerShell:
$env:NBA_API_KEY = "your_key"

Place kaggle.json in C:\Users\<username>\.kaggle\ for Windows or ~/.kaggle/ for Mac/Linux
kaggle.json
{"username": "your_username, "key": "your_kaggle_key"}


## Analysis
1. Top 10 highest scoring games in the last decade (screenshots/Top10.png)
2. Win/Loss record for each team over the last decade (screenshots/WinLossRecords.png)
3. Average points scored by each team per season over the last decade (screenshots/AVGPoints.png)
4. Eastern VS Western conference wins (screenshots/EastVSWest.png)
5. Team with the highest average margin of victory in the last decade (screenshots/MarginOfVictory.png)
6. Average points scored/allowed by each team and season (screenshots/AVGPointsScoredAllowed.png)

