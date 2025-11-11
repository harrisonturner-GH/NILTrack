# generate_report.py
import sqlite3, datetime, pathlib, html

DB = "cbb_tracker.sqlite"
PLAYERS_FILE = "players.txt"
OUT = pathlib.Path("site")
OUT.mkdir(parents=True, exist_ok=True)

SEASON_YEAR = 2026  # 2025-26 season end-year
OK = ("post", "final", "status_final")

def load_watchlist():
    p = pathlib.Path(PLAYERS_FILE)
    if not p.exists():
        return []
    return [ln.strip() for ln in p.read_text().splitlines() if ln.strip() and not ln.startswith("#")]

def q(con, sql, *args):
    cur = con.execute(sql, args)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def table(rows, headers):
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    trs = []
    for r in rows:
        tds = "".join(f"<td>{html.escape(str(r.get(h,'')))}</td>" for h in headers)
        trs.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(trs)}</tbody></table>"

def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row

    names = load_watchlist()
    blocks = []
    summary_lines = []

    # simple league sanity
    games_post = q(con, "SELECT COUNT(*) AS n FROM games WHERE season_year=? AND lower(status)='post'", SEASON_YEAR)[0]["n"]

    for name in names:
        # resolve player(s) by exact, else partial
        rows = q(con, "SELECT player_id, player_name, team_id FROM players WHERE lower(player_name)=lower(?)", name.lower())
        if not rows:
            rows = q(con, "SELECT player_id, player_name, team_id FROM players WHERE lower(player_name) LIKE ?", f"%{name.lower()}%")
        if not rows:
            blocks.append(f"<h2>{html.escape(name)}</h2><p><em>Not found in DB yet (maybe no completed games)</em></p>")
            continue

        for r in rows:
            pid, pname, tid = r["player_id"], r["player_name"], r["team_id"]
            team = q(con, "SELECT team_name FROM teams WHERE team_id=?", tid)
            team_name = team[0]["team_name"] if team else str(tid)

            # recent 10 games
            recent = q(con, """
                SELECT substr(g.date,1,10) AS DATE,
                       g.away_team || ' @ ' || g.home_team AS MATCHUP,
                       s.minutes AS MIN, s.pts AS PTS, s.reb AS REB, s.ast AS AST,
                       s.stl AS STL, s.blk AS BLK
                FROM player_game_stats s
                JOIN games g ON g.game_id = s.game_id
                WHERE s.player_id=? AND g.season_year=? AND lower(g.status) IN ('post','final','status_final')
                ORDER BY g.date DESC LIMIT 10
            """, pid, SEASON_YEAR)

            # season averages
            avg = q(con, """
                SELECT ROUND(AVG(s.pts),1) AS PPG,
                       ROUND(AVG(s.reb),1) AS RPG,
                       ROUND(AVG(s.ast),1) AS APG,
                       ROUND(AVG(s.stl),1) AS SPG,
                       ROUND(AVG(s.blk),1) AS BPG,
                       COUNT(*) AS GP
                FROM player_game_stats s
                JOIN games g ON g.game_id = s.game_id
                WHERE s.player_id=? AND g.season_year=? AND lower(g.status) IN ('post','final','status_final')
            """, pid, SEASON_YEAR)[0]

            # “played in last 24h” highlight
            changed = ""
            if recent:
                try:
                    most_recent_date = datetime.date.fromisoformat(recent[0]["DATE"])
                    if (datetime.date.today() - most_recent_date).days <= 1:
                        g0 = recent[0]
                        changed = f"Played {g0['DATE']}: {g0['PTS']}p {g0['REB']}r {g0['AST']}a vs {g0['MATCHUP']}"
                        summary_lines.append(f"{pname} ({team_name}) — {changed.split(':',1)[1].strip()}")
                except Exception:
                    pass

            blocks.append(
                f"<h2>{html.escape(pname)} — {html.escape(team_name)}</h2>"
                f"<p>{html.escape(changed) if changed else '<em>No new game in last 24h</em>'}</p>"
                f"<p>Season: <strong>{avg['PPG']}</strong> PPG / <strong>{avg['RPG']}</strong> RPG / "
                f"<strong>{avg['APG']}</strong> APG (GP {avg['GP']})</p>"
                + table(recent, ["DATE","MATCHUP","MIN","PTS","REB","AST","STL","BLK"])
            )

    # write HTML page
    html_out = f"""<!doctype html><html><head><meta charset="utf-8">
    <title>Daily Player Report</title>
    <style>body{{font-family:system-ui,Arial;margin:24px;}} table{{border-collapse:collapse;width:100%;margin:12px 0}}
    th,td{{border:1px solid #ddd;padding:6px;text-align:center}} th{{background:#f2f2f2}}</style></head>
    <body>
    <h1>Daily Player Report</h1>
    <p>Updated: {datetime.datetime.utcnow().isoformat(timespec='seconds')}Z • Season {SEASON_YEAR} • Post games: {games_post}</p>
    {''.join(blocks) if blocks else '<p><em>No players in watchlist.</em></p>'}
    </body></html>"""
    (OUT / "index.html").write_text(html_out, encoding="utf-8")

    # write summary.txt for chat/webhook
    summary = "Daily update:\n" + ("\n".join(summary_lines) if summary_lines else "No new games in last 24h.")
    (OUT / "summary.txt").write_text(summary, encoding="utf-8")

if __name__ == "__main__":
    main()
