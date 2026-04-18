import duckdb
import streamlit as st
import plotly.express as px


def load_tables(con):
    con.execute("""
        CREATE OR REPLACE TABLE games AS
        SELECT * FROM read_parquet('data/cleaned_combined_games.parquet')
    """)
    con.execute("""
        CREATE OR REPLACE TABLE teams AS
        SELECT * FROM read_parquet('data/cleaned_teams.parquet')
    """)
    con.execute("""
        CREATE OR REPLACE TABLE team_games AS
        SELECT season, home_team AS team, home_score AS team_score, away_score AS opponent_score FROM games
        WHERE date >= current_date - INTERVAL '10 years'
        UNION ALL
        SELECT season, away_team AS team, away_score AS team_score, home_score AS opponent_score FROM games
        WHERE date >= current_date - INTERVAL '10 years'
    """)

    # Created Indexes on team and season for team_games and games tables
    con.execute("CREATE INDEX idx_team_games_team ON team_games (team)")
    con.execute("CREATE INDEX idx_team_games_season ON team_games (season)")
    con.execute("CREATE INDEX idx_games_season ON games (season)")


def top_scoring_games(con):
    return con.execute("""
        WITH games1 AS (
            SELECT date, home_team, away_team, (home_score + away_score) AS total_score
            FROM games
            WHERE date >= current_date - INTERVAL '10 years'
        )
        SELECT date, home_team, away_team, total_score,
            DENSE_RANK() OVER (ORDER BY total_score DESC) AS rnk
        FROM games1
        LIMIT 10
    """).df()


def win_loss_records(con):
    return con.execute("""
        WITH team_games_wins AS (
            SELECT team,
                -- This does not include ties
                CASE
                    WHEN team_score > opponent_score THEN 1
                    WHEN team_score < opponent_score THEN 0
                END AS wins
            FROM team_games
        )
        SELECT team, SUM(wins) AS wins, COUNT(*) - SUM(wins) AS losses
        FROM team_games_wins
        GROUP BY team
        ORDER BY team
    """).df()


def team_performance_by_season(con):
    return con.execute("""
        SELECT season, team, AVG(team_score) AS avg_score
        FROM team_games
        GROUP BY season, team
        ORDER BY season, team
    """).df()


def conference_wins(con):
    return con.execute("""
        WITH team_games_wins AS (
            SELECT team,
                CASE
                    WHEN team_score > opponent_score THEN 1
                    WHEN team_score < opponent_score THEN 0
                END AS wins
            FROM team_games
        ),
        total_wins AS (
            SELECT team, SUM(wins) AS wins, COUNT(*) - SUM(wins) AS losses
            FROM team_games_wins
            GROUP BY team
        )
        SELECT t.conference, SUM(wins) AS wins
        FROM total_wins w
        JOIN teams t ON w.team = t.team_name
        GROUP BY conference
    """).df()


def margin_of_victory(con):
    return con.execute("""
        SELECT team, AVG(team_score - opponent_score) AS avg_margin
        FROM team_games
        WHERE team_score > opponent_score
        GROUP BY team
        ORDER BY avg_margin DESC
        LIMIT 1
    """).df()


def team_performance_trends(con):
    return con.execute("""
        SELECT team, season, AVG(team_score) AS avg_scored, AVG(opponent_score) AS avg_allowed
        FROM team_games
        GROUP BY team, season
        ORDER BY team, season
    """).df()


def main():
    con = duckdb.connect("hadrian.duckdb")
    load_tables(con)

    st.title("NBA Analysis")

    st.header("Top 10 Highest Scoring Games")
    result = top_scoring_games(con)
    fig = px.scatter(result, x="date", y="total_score", color="rnk")
    st.plotly_chart(fig)

    st.header("Win/Loss Records")
    result2 = win_loss_records(con)
    fig2 = px.bar(result2, x="team", y=["wins", "losses"], barmode="stack")
    st.plotly_chart(fig2)

    st.header("Team Performance by Season")
    result3 = team_performance_by_season(con)
    teams = result3["team"].unique()
    selected_team = st.selectbox("Select a team", sorted(teams), key="team_select_r3")
    filtered = result3[result3["team"] == selected_team]
    fig3 = px.bar(filtered, x="season", y="avg_score",
                  title=f"{selected_team} - Avg Points Per Season")
    st.plotly_chart(fig3)

    st.header("East VS West")
    result4 = conference_wins(con)
    fig4 = px.pie(result4, values="wins", names="conference")
    st.plotly_chart(fig4)

    st.header("Margin of Victory")
    result5 = margin_of_victory(con)
    st.metric(label=result5["team"][0], value=round(result5["avg_margin"][0], 2))

    st.header("Team Performance")
    result6 = team_performance_trends(con)
    teams = result6["team"].unique()
    selected_team2 = st.selectbox("Select a team", sorted(teams), key="team_select_r6")
    filtered = result6[result6["team"] == selected_team2]
    fig6 = px.line(filtered, x="season", y=["avg_scored", "avg_allowed"],
                   title=f"{selected_team2} - Avg Points Scored vs Allowed")
    st.plotly_chart(fig6)


if __name__ == "__main__":
    main()