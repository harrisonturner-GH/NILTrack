import pandas as pd, sqlite3, streamlit as st
st.set_page_config(page_title="CBB Tracker", layout="wide")
con = sqlite3.connect("cbb_tracker.sqlite")
st.title("College Basketball Player Tracker")
tab1, tab2 = st.tabs(["Season Averages", "Raw Game Log"])

df = pd.read_sql_query("""
  SELECT p.player_name AS Player, t.team_name AS Team,
         ROUND(AVG(pts),1) AS PPG, ROUND(AVG(reb),1) AS RPG, ROUND(AVG(ast),1) AS APG,
         ROUND(AVG(stl),1) AS SPG, ROUND(AVG(blk),1) AS BPG, ROUND(AVG(tov),1) AS TOV
  FROM player_game_stats s
  JOIN players p ON p.player_id = s.player_id
  JOIN teams t ON t.team_id = p.team_id
  GROUP BY p.player_id, t.team_name, p.player_name
  ORDER BY PPG DESC
""", con)
tab1.dataframe(df, use_container_width=True)

log = pd.read_sql_query("""
  SELECT g.date, p.player_name AS Player, t.team_name AS Team, 
         s.pts, s.reb, s.ast, s.stl, s.blk, s.tov, s.fgm, s.fga, s.tpm, s.tpa, s.ftm, s.fta, s.minutes, s.game_id
  FROM player_game_stats s
  JOIN players p ON p.player_id = s.player_id
  JOIN teams t ON t.team_id = p.team_id
  JOIN games g ON g.game_id = s.game_id
  ORDER BY g.date DESC
""", con)
tab2.dataframe(log, use_container_width=True)
