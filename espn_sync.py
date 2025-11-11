import sqlite3, time, re
from pathlib import Path
import requests, yaml

DB = "cbb_tracker.sqlite"
CFG = "players.yaml"

DUKE_ID = "150"  # Duke
TEAM_INFO  = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}"
TEAM_SCHED = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}/schedule?season={season}"
EVENT_SUM  = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={event_id}"

sess = requests.Session()
sess.headers.update({"User-Agent":"Mozilla/5.0 (Mac) CBBTracker/1.0"})

def j(url):
    r = sess.get(url, timeout=20); r.raise_for_status(); return r.json()

def db():
    con = sqlite3.connect(DB); cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS teams(team_id TEXT PRIMARY KEY, team_name TEXT, team_slug TEXT);
    CREATE TABLE IF NOT EXISTS players(player_id TEXT PRIMARY KEY, player_name TEXT, team_id TEXT, UNIQUE(player_name, team_id));
    CREATE TABLE IF NOT EXISTS games(game_id TEXT PRIMARY KEY, date TEXT, home_team TEXT, away_team TEXT, status TEXT, season_year INTEGER);
    CREATE TABLE IF NOT EXISTS player_game_stats(
      player_id TEXT, game_id TEXT, minutes TEXT,
      pts INTEGER, reb INTEGER, ast INTEGER, stl INTEGER, blk INTEGER, tov INTEGER,
      fgm INTEGER, fga INTEGER, tpm INTEGER, tpa INTEGER, ftm INTEGER, fta INTEGER,
      PRIMARY KEY(player_id, game_id)
    );
    """); con.commit(); return con, cur

def upsert_game(cur, con, g):
    cur.execute("""INSERT OR REPLACE INTO games(game_id,date,home_team,away_team,status,season_year)
                   VALUES(?,?,?,?,?,?)""",
                (g["game_id"], g["date"], g["home"], g["away"], g.get("status",""), g.get("season_year"))); 
    con.commit()

def upsert_player(cur, con, player_name, team_id, player_id):
    cur.execute("INSERT OR IGNORE INTO players(player_id, player_name, team_id) VALUES(?,?,?)",
                (player_id, player_name, team_id)); con.commit()

def upsert_line(cur, con, pid, gid, row):
    vals = (pid, gid, row.get("minutes","") or row.get("min",""),
            int(row.get("points",0)), int(row.get("rebounds",0)), int(row.get("assists",0)),
            int(row.get("steals",0)), int(row.get("blocks",0)), int(row.get("turnovers",0)),
            int(row.get("fieldGoalsMade",0)), int(row.get("fieldGoalsAttempted",0)),
            int(row.get("threePointFieldGoalsMade",0)), int(row.get("threePointFieldGoalsAttempted",0)),
            int(row.get("freeThrowsMade",0)), int(row.get("freeThrowsAttempted",0)))
    cur.execute("""INSERT OR REPLACE INTO player_game_stats
      (player_id, game_id, minutes, pts, reb, ast, stl, blk, tov, fgm, fga, tpm, tpa, ftm, fta)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", vals); con.commit()

def season_to_year(season_str):
    # "2025-26" -> 2026 (ESPN uses end year)
    m = re.match(r"(\d{4})-(\d{2})", str(season_str or ""))
    if m: return int(m.group(1)) + 1
    # fallback to current season end-year if unspecified
    return 2026

def main():
    cfg = yaml.safe_load(open(CFG))
    season_year = season_to_year(cfg.get("season") or "2025-26")

    con, cur = db()
    ti = j(TEAM_INFO.format(id=DUKE_ID))
    disp = (ti.get("team",{}) or {}).get("displayName") or "Duke Blue Devils"
    slug = (ti.get("team",{}) or {}).get("slug") or ""
    cur.execute("INSERT OR IGNORE INTO teams(team_id, team_name, team_slug) VALUES(?,?,?)",
                (DUKE_ID, disp, slug)); con.commit()

    # collect tracked names for Duke (case-insensitive)
    tracked = {p["name"].lower() for p in cfg.get("players", []) if p.get("team","").lower()=="duke"}

    sched = j(TEAM_SCHED.format(id=DUKE_ID, season=season_year))
    events = sched.get("events") or []
    print(f"[info] {disp}: season={season_year} schedule events = {len(events)}")
    completed = 0
    matched = set()

    for ev in events:
        eid = ev.get("id")
        comp = (ev.get("competitions") or [{}])[0]
        date = ev.get("date","")
        comps = comp.get("competitors",[{}])
        home = (comps[0].get("team",{}) or {}).get("displayName","")
        away = (comps[1].get("team",{}) or {}).get("displayName","")
        typ  = (comp.get("status",{}) or {}).get("type",{}) or {}
        state = (typ.get("state","") or "").lower()
        status = typ.get("description","") or typ.get("name","") or state

        # Only count games that ESPN says belong to this season
        ev_season = (ev.get("season") or {}).get("year")
        if ev_season and int(ev_season) != int(season_year):
            continue

        upsert_game(cur, con, {"game_id": eid, "date": date, "home": home, "away": away, "status": status, "season_year": season_year})

        # Completed only
        if state != "post":
            continue
        completed += 1

        try:
            summ = j(EVENT_SUM.format(event_id=eid))
        except Exception as e:
            print(f"[warn] summary fetch failed for {eid}: {e}")
            continue

        box = summ.get("boxscore") or {}

        def handle_rows(rows):
            for row in rows or []:
                ath = (row.get("athlete") or {})
                nm  = (ath.get("displayName") or "").strip()
                pid = str(ath.get("id") or nm)
                if not nm: 
                    continue
                if nm.lower() in tracked:
                    stats = {s["name"]: s.get("value",0) for s in (row.get("stats") or []) if isinstance(s, dict) and "name" in s}
                    mapped = {
                        "points": stats.get("points",0),
                        "rebounds": stats.get("rebounds",0),
                        "assists": stats.get("assists",0),
                        "steals": stats.get("steals",0),
                        "blocks": stats.get("blocks",0),
                        "turnovers": stats.get("turnovers",0),
                        "fieldGoalsMade": stats.get("fieldGoalsMade",0),
                        "fieldGoalsAttempted": stats.get("fieldGoalsAttempted",0),
                        "threePointFieldGoalsMade": stats.get("threePointFieldGoalsMade",0),
                        "threePointFieldGoalsAttempted": stats.get("threePointFieldGoalsAttempted",0),
                        "freeThrowsMade": stats.get("freeThrowsMade",0),
                        "freeThrowsAttempted": stats.get("freeThrowsAttempted",0),
                        "minutes": stats.get("minutes",""),
                    }
                    upsert_player(cur, con, nm, DUKE_ID, pid)
                    upsert_line(cur, con, pid, eid, mapped)
                    matched.add(nm)

        # Shape A: teams[].statistics[].athletes[]  — filter to Duke side only
        for side in (box.get("teams") or []):
            team_meta = (side.get("team") or {})
            if str(team_meta.get("id")) != DUKE_ID:
                continue
            for sgrp in (side.get("statistics") or []):
                handle_rows(sgrp.get("athletes"))

        # Shape B: players[].statistics[].athletes[] — also filter to Duke team
        for pl in (box.get("players") or []):
            team_meta = (pl.get("team") or {})
            if str(team_meta.get("id")) != DUKE_ID:
                continue
            for stat in (pl.get("statistics") or []):
                handle_rows(stat.get("athletes"))

        time.sleep(0.02)

    print(f"[info] {disp}: completed games (this season) = {completed}, matched players this run = {sorted(list(matched))}")
    print("ESPN sync complete. DB:", Path(DB).resolve())

if __name__ == "__main__":
    main()
