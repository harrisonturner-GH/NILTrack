import re, time, csv, json
from pathlib import Path
import requests, sqlite3, argparse

TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?groups=50&limit=1000"
TEAM_INFO   = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}"
TEAM_SCHED  = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}/schedule?season={season}"
EVENT_SUM   = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={event_id}"

def season_to_year(s):
    m = re.match(r"^\s*(\d{4})\s*-\s*(\d{2})\s*$", s or "")
    if m: return int(m.group(1)) + 1
    m = re.match(r"^\s*(\d{4})\s*$", s or "")
    if m: return int(m.group(1))
    raise SystemExit("Season must look like '2025-26' or '2026'.")

def sess():
    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0 CBBTracker/1.0"})
    return s

def j(s, url):
    r = s.get(url, timeout=20); r.raise_for_status(); return r.json()

def fg_pair(s):
    try: a,b = s.split('-'); return int(a), int(b)
    except: return 0,0

def parse_row(group, row):
    labels = group.get("names") or group.get("labels") or []
    vals   = row.get("stats") or []
    m = { (labels[i] if i < len(labels) else f"col{i}") : vals[i] for i in range(min(len(labels), len(vals))) }

    minutes   = m.get("minutes") or m.get("MIN") or ""
    points    = int(m.get("points") or m.get("PTS") or 0)
    rebounds  = int(m.get("rebounds") or m.get("REB") or 0)
    assists   = int(m.get("assists")  or m.get("AST") or 0)
    steals    = int(m.get("steals")   or m.get("STL") or 0)
    blocks    = int(m.get("blocks")   or m.get("BLK") or 0)
    turnovers = int(m.get("turnovers")or m.get("TO")  or 0)

    if "fieldGoalsMade" in m or "threePointFieldGoalsMade" in m or "freeThrowsMade" in m:
        fgm = int(m.get("fieldGoalsMade",0) or 0); fga = int(m.get("fieldGoalsAttempted",0) or 0)
        tpm = int(m.get("threePointFieldGoalsMade",0) or 0); tpa = int(m.get("threePointFieldGoalsAttempted",0) or 0)
        ftm = int(m.get("freeThrowsMade",0) or 0); fta = int(m.get("freeThrowsAttempted",0) or 0)
    else:
        fgm,fga = fg_pair(m.get("FG","0-0")); tpm,tpa = fg_pair(m.get("3PT","0-0")); ftm,fta = fg_pair(m.get("FT","0-0"))

    return {"minutes":minutes,"points":points,"rebounds":rebounds,"assists":assists,"steals":steals,"blocks":blocks,
            "turnovers":turnovers,"fgm":fgm,"fga":fga,"tpm":tpm,"tpa":tpa,"ftm":ftm,"fta":fta}

def ensure_schema(con):
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS teams(
      team_id TEXT PRIMARY KEY,
      team_name TEXT,
      team_slug TEXT,
      conference TEXT
    );
    CREATE TABLE IF NOT EXISTS games(
      game_id TEXT PRIMARY KEY,
      date TEXT,
      home_team TEXT,
      away_team TEXT,
      status TEXT,
      season_year INTEGER
    );
    CREATE TABLE IF NOT EXISTS players(
      player_id TEXT PRIMARY KEY,
      player_name TEXT,
      team_id TEXT
    );
    CREATE TABLE IF NOT EXISTS player_game_stats(
      player_id TEXT,
      game_id TEXT,
      minutes TEXT,
      pts INTEGER, reb INTEGER, ast INTEGER, stl INTEGER, blk INTEGER, tov INTEGER,
      fgm INTEGER, fga INTEGER, tpm INTEGER, tpa INTEGER, ftm INTEGER, fta INTEGER,
      PRIMARY KEY(player_id, game_id)
    );
    """)
    con.commit()

def main():
    ap = argparse.ArgumentParser(description="Sync ALL teams' completed box scores into SQLite.")
    ap.add_argument("--season", required=True, help="Season like '2025-26' or '2026'")
    ap.add_argument("--db", default="cbb_tracker.sqlite")
    ap.add_argument("--conference", action="append", default=[],
                    help="Optional conference name filter(s), case-insensitive substring match. Repeatable.")
    ap.add_argument("--max-teams", type=int, default=0, help="Process only first N matching teams (for testing).")
    ap.add_argument("--sleep", type=float, default=0.05, help="Delay between event summary requests (seconds).")
    args = ap.parse_args()

    season_year = season_to_year(args.season)
    s = sess()
    con = sqlite3.connect(args.db)
    ensure_schema(con)
    cur = con.cursor()

    data = j(s, TEAMS_URL)
    teams = []
    for sp in data.get("sports", []):
        for lg in sp.get("leagues", []):
            conf_name = (lg.get("name") or "").strip()
            for t in lg.get("teams", []):
                info = t.get("team", {}) or {}
                tid = str(info.get("id"))
                name = info.get("displayName") or info.get("name") or ""
                slug = info.get("slug") or info.get("abbreviation") or ""
                if args.conference:
                    hay = conf_name.lower()
                    if not any(c.lower() in hay for c in args.conference):
                        continue
                teams.append((tid, name, slug, conf_name))
    teams.sort(key=lambda x: x[1].lower())

    if args.max_teams > 0:
        teams = teams[:args.max_teams]

    print(f"[info] matching teams: {len(teams)} • season={season_year}")
    total_rows = total_games = 0

    for i,(tid, name, slug, conf) in enumerate(teams, 1):
        # upsert team
        cur.execute("INSERT OR IGNORE INTO teams(team_id,team_name,team_slug,conference) VALUES(?,?,?,?)",
                    (tid, name, slug, conf))
        con.commit()

        try:
            sched = j(s, TEAM_SCHED.format(id=tid, season=season_year))
        except Exception as e:
            print(f"[warn] schedule failed for {name} ({tid}): {e}")
            continue

        events = sched.get("events") or []
        completed = 0; inserted = 0
        for ev in events:
            comp = (ev.get("competitions") or [{}])[0]
            typ  = (comp.get("status") or {}).get("type",{}) or {}
            state = (typ.get("state","") or "").lower()
            if state != "post":
                continue
            ev_season = (ev.get("season") or {}).get("year")
            if ev_season and int(ev_season) != int(season_year):
                continue

            gid  = ev.get("id")
            date = ev.get("date","")
            comps = comp.get("competitors",[{}])
            home = (comps[0].get("team",{}) or {}).get("displayName","")
            away = (comps[1].get("team",{}) or {}).get("displayName","")
            status = typ.get("description","") or typ.get("name","") or state

            # upsert game
            cur.execute("""INSERT OR REPLACE INTO games(game_id,date,home_team,away_team,status,season_year)
                           VALUES(?,?,?,?,?,?)""",
                        (gid, date, home, away, status, season_year))
            con.commit()

            # fetch boxscore summary
            try:
                summ = j(s, EVENT_SUM.format(event_id=gid))
            except Exception as e:
                print(f"[warn] summary failed for game {gid} {name}: {e}")
                continue

            box = summ.get("boxscore") or {}

            def record_row(team_meta, group, row):
                nonlocal inserted
                ath = (row.get("athlete") or {})
                nm  = (ath.get("displayName") or "").strip()
                if not nm: return
                pid = str(ath.get("id") or nm)
                stats = parse_row(group, row)
                # upsert player
                cur.execute("INSERT OR IGNORE INTO players(player_id,player_name,team_id) VALUES(?,?,?)",
                            (pid, nm, str((team_meta or {}).get("id") or "")))
                # upsert line
                cur.execute("""INSERT OR REPLACE INTO player_game_stats
                   (player_id, game_id, minutes, pts, reb, ast, stl, blk, tov, fgm, fga, tpm, tpa, ftm, fta)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                   (pid, gid, stats["minutes"], stats["points"], stats["rebounds"], stats["assists"],
                    stats["steals"], stats["blocks"], stats["turnovers"], stats["fgm"], stats["fga"],
                    stats["tpm"], stats["tpa"], stats["ftm"], stats["fta"]))
                inserted += 1

            # shape A
            for side in (box.get("teams") or []):
                tmeta = side.get("team") or {}
                for grp in (side.get("statistics") or []):
                    for row in (grp.get("athletes") or []):
                        record_row(tmeta, grp, row)
            # shape B
            for pl in (box.get("players") or []):
                tmeta = pl.get("team") or {}
                for grp in (pl.get("statistics") or []):
                    for row in (grp.get("athletes") or []):
                        record_row(tmeta, grp, row)

            completed += 1
            time.sleep(args.sleep)

        total_rows += inserted
        total_games += completed
        print(f"[{i:03d}/{len(teams)}] {name}: completed={completed}, rows={inserted}")

    print(f"[done] games inserted: {total_games}, player lines inserted: {total_rows}")
    # normalize status for your CLI
    cur.execute("UPDATE games SET status='post' WHERE lower(status) IN ('final','status_final')")
    con.commit()

if __name__ == "__main__":
    main()
