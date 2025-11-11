import argparse, csv, json, re, time
from pathlib import Path
import requests

# Endpoints
TEAMS_URL   = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
TEAM_INFO   = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}"
TEAM_SCHED  = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{id}/schedule?season={season}"
EVENT_SUM   = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={event_id}"

# Some handy defaults
TEAM_ID_MAP = {
    "duke": "150",
}

# ---- helpers ----

def season_to_year(season_str: str) -> int:
    # "2025-26" -> 2026  (ESPN uses the end year)
    m = re.match(r"^\s*(\d{4})\s*-\s*(\d{2})\s*$", season_str or "")
    if m:
        return int(m.group(1)) + 1
    # If user passed "2026" directly
    m = re.match(r"^\s*(\d{4})\s*$", season_str or "")
    if m:
        return int(m.group(1))
    raise SystemExit(f"Season '{season_str}' not understood (try '2025-26').")

def session():
    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0 (Mac) CBBTracker/1.0"})
    return s

def j(sess, url):
    r = sess.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def resolve_team_id(sess, team_name_or_id: str) -> (str, str):
    # If user already passed digits, assume it's an ID.
    if team_name_or_id.isdigit():
        tid = team_name_or_id
        info = j(sess, TEAM_INFO.format(id=tid)).get("team",{}) or {}
        slug = info.get("slug") or info.get("shortDisplayName") or info.get("displayName") or tid
        return tid, slug

    key = team_name_or_id.strip().lower()
    if key in TEAM_ID_MAP:
        tid = TEAM_ID_MAP[key]
        info = j(sess, TEAM_INFO.format(id=tid)).get("team",{}) or {}
        slug = info.get("slug") or info.get("shortDisplayName") or info.get("displayName") or tid
        return tid, slug

    # Fallback: search full team list
    data = j(sess, TEAMS_URL)
    for sp in data.get("sports", []):
        for lg in sp.get("leagues", []):
            for t in lg.get("teams", []):
                info = t.get("team", {}) or {}
                fields = " ".join([
                    str(info.get("id") or ""),
                    str(info.get("displayName") or ""),
                    str(info.get("shortDisplayName") or ""),
                    str(info.get("abbreviation") or ""),
                    str(info.get("location") or ""),
                    str(info.get("name") or ""),
                ]).lower()
                if key in fields:
                    tid = str(info.get("id"))
                    slug = info.get("slug") or info.get("shortDisplayName") or info.get("abbreviation") or info.get("name") or tid
                    return tid, slug
    raise SystemExit(f"Could not resolve team '{team_name_or_id}'. Try --team-id 150 for Duke.")

def extract_rows_from_boxscore(box: dict) -> list:
    """Return a list of dict rows (one per player line) for BOTH teams."""
    rows = []

    def take(team_meta, athlete_row, stat_map):
        ath = (athlete_row.get("athlete") or {})
        nm  = (ath.get("displayName") or "").strip()
        if not nm:
            return
        pid = str(ath.get("id") or nm)
        # build row
        out = {
            "team_id": str((team_meta or {}).get("id") or ""),
            "team_name": (team_meta or {}).get("displayName") or "",
            "player_id": pid,
            "player_name": nm,
            "minutes": stat_map.get("minutes",""),
            "pts": int(stat_map.get("points",0) or 0),
            "reb": int(stat_map.get("rebounds",0) or 0),
            "ast": int(stat_map.get("assists",0) or 0),
            "stl": int(stat_map.get("steals",0) or 0),
            "blk": int(stat_map.get("blocks",0) or 0),
            "tov": int(stat_map.get("turnovers",0) or 0),
            "fgm": int(stat_map.get("fieldGoalsMade",0) or 0),
            "fga": int(stat_map.get("fieldGoalsAttempted",0) or 0),
            "tpm": int(stat_map.get("threePointFieldGoalsMade",0) or 0),
            "tpa": int(stat_map.get("threePointFieldGoalsAttempted",0) or 0),
            "ftm": int(stat_map.get("freeThrowsMade",0) or 0),
            "fta": int(stat_map.get("freeThrowsAttempted",0) or 0),
        }
        rows.append(out)

    # shape A: box["teams"][i]["statistics"][j]["athletes"][k]
    for side in (box.get("teams") or []):
        tmeta = side.get("team") or {}
        for sgrp in (side.get("statistics") or []):
            for row in (sgrp.get("athletes") or []):
                stats = {s["name"]: s.get("value",0) for s in (row.get("stats") or []) if isinstance(s, dict) and "name" in s}
                take(tmeta, row, stats)

    # shape B: box["players"][i]["statistics"][j]["athletes"][k]
    for pl in (box.get("players") or []):
        tmeta = pl.get("team") or {}
        for sgrp in (pl.get("statistics") or []):
            for row in (sgrp.get("athletes") or []):
                stats = {s["name"]: s.get("value",0) for s in (row.get("stats") or []) if isinstance(s, dict) and "name" in s}
                take(tmeta, row, stats)

    return rows

def main():
    ap = argparse.ArgumentParser(description="Dump complete ESPN box scores for a team/season to CSV.")
    ap.add_argument("--team", help="Team name (e.g., 'Duke'). You can also use --team-id.", default="Duke")
    ap.add_argument("--team-id", help="ESPN team id (e.g., 150 for Duke). Overrides --team.")
    ap.add_argument("--season", required=True, help="Season like '2025-26' (or '2026').")
    ap.add_argument("--outdir", default="boxscores_out", help="Output directory.")
    ap.add_argument("--save-json", action="store_true", help="Also save per-game raw JSON.")
    args = ap.parse_args()

    s = session()
    season_year = season_to_year(args.season)
    tid, slug = resolve_team_id(s, args.team_id if args.team_id else args.team)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / f"boxscores_{slug}_{season_year}.csv"

    # Write CSV header
    fieldnames = [
        "game_id","date","status","home_team","away_team","team_id","team_name",
        "player_id","player_name","minutes","pts","reb","ast","stl","blk","tov","fgm","fga","tpm","tpa","ftm","fta"
    ]
    fcsv = open(csv_path, "w", newline="")
    writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
    writer.writeheader()

    # Pull schedule
    sched = j(s, TEAM_SCHED.format(id=tid, season=season_year))
    events = sched.get("events") or []
    print(f"[info] {slug}: season={season_year} schedule events = {len(events)}")

    completed = 0
    total_rows = 0
    for ev in events:
        # season sanity
        ev_season = (ev.get("season") or {}).get("year")
        if ev_season and int(ev_season) != int(season_year):
            continue

        comp = (ev.get("competitions") or [{}])[0]
        typ  = (comp.get("status",{}) or {}).get("type",{}) or {}
        state = (typ.get("state","") or "").lower()
        if state != "post":
            continue  # only completed games
        completed += 1

        gid  = ev.get("id")
        date = ev.get("date","")
        comps = comp.get("competitors",[{}])
        home = (comps[0].get("team",{}) or {}).get("displayName","")
        away = (comps[1].get("team",{}) or {}).get("displayName","")
        status = typ.get("description","") or typ.get("name","") or state

        # fetch summary
        summ = j(s, EVENT_SUM.format(event_id=gid))
        box = summ.get("boxscore") or {}

        if args.save_json:
            (outdir / f"{gid}.summary.json").write_text(json.dumps(summ, indent=2))

        rows = extract_rows_from_boxscore(box)
        for r in rows:
            writer.writerow({
                "game_id": gid, "date": date, "status": status,
                "home_team": home, "away_team": away,
                **r
            })
        total_rows += len(rows)
        time.sleep(0.05)

    fcsv.close()
    print(f"[info] completed games: {completed}")
    print(f"[info] wrote rows: {total_rows} -> {csv_path.resolve()}")

if __name__ == "__main__":
    main()
