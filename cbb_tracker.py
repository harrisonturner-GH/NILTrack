import sqlite3, time, json
from pathlib import Path
from typing import Optional, List
import requests, typer, pandas as pd, yaml
from rich.console import Console
from rich.table import Table
from rich import box

app = typer.Typer(help="College hoops tracker: schedules + player stats")
console = Console()

NCAA_BASE = "https://ncaa-api.henrygd.me"   # can be self-hosted
DB_PATH = Path("cbb_tracker.sqlite")
CFG_PATH = Path("players.yaml")

def get_json(path: str):
    url = f"{NCAA_BASE}{path}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def load_cfg():
    if not CFG_PATH.exists():
        raise SystemExit("players.yaml not found. Create it first.")
    with open(CFG_PATH, "r") as f:
        return yaml.safe_load(f)

def db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS teams(
      team_id TEXT PRIMARY KEY,
      team_name TEXT,
      team_slug TEXT
    );
    CREATE TABLE IF NOT EXISTS players(
      player_id TEXT PRIMARY KEY,
      player_name TEXT,
      team_id TEXT,
      UNIQUE(player_name, team_id)
    );
    CREATE TABLE IF NOT EXISTS games(
      game_id TEXT PRIMARY KEY,
      date TEXT,
      home_team TEXT,
      away_team TEXT,
      status TEXT
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
    return con, cur

def schools_index():
    return get_json("/schools-index")

def team_to_slug(team_name: str, index: list[str]):
    for s in index:
        if s.get("name","").lower() == team_name.lower():
            return s.get("slug")
    for s in index:
        if team_name.lower() in s.get("name","").lower():
            return s.get("slug")
    return None

def save_team(cur, con, team_name, slug):
    tid = slug
    cur.execute("INSERT OR IGNORE INTO teams(team_id,team_name,team_slug) VALUES(?,?,?)",
                (tid, team_name, slug))
    con.commit()
    return tid

def team_schedule(slug: str, season: str, division: str):
    sport = "basketball-men" if division == "mens" else "basketball-women"
    try_paths = [
        f"/schools/{slug}/{sport}/{season}/schedule",
        f"/schools/{slug}/{sport}/schedule",
    ]
    for p in try_paths:
        try:
            j = get_json(p)
            if j:
                return j
        except Exception:
            continue
    return {}

def game_box(game_id: str):
    return get_json(f"/game/{game_id}/boxscore")

def upsert_game(cur, con, g):
    cur.execute("INSERT OR REPLACE INTO games(game_id,date,home_team,away_team,status) VALUES(?,?,?,?,?)",
                (g["game_id"], g["date"], g["home"], g["away"], g.get("status","")))
    con.commit()

def upsert_player(cur, con, player_name, team_id, player_id):
    cur.execute("INSERT OR IGNORE INTO players(player_id, player_name, team_id) VALUES(?,?,?)",
                (player_id, player_name, team_id))
    con.commit()

def upsert_line(cur, con, pid, gid, row):
    vals = (
        pid, gid, row.get("min",""),
        int(row.get("pts",0)), int(row.get("reb",0)), int(row.get("ast",0)),
        int(row.get("stl",0)), int(row.get("blk",0)), int(row.get("to",0)),
        int(row.get("fgm",0)), int(row.get("fga",0)),
        int(row.get("tpm",0)), int(row.get("tpa",0)),
        int(row.get("ftm",0)), int(row.get("fta",0)),
    )
    cur.execute("""
      INSERT OR REPLACE INTO player_game_stats
      (player_id, game_id, minutes, pts, reb, ast, stl, blk, tov, fgm, fga, tpm, tpa, ftm, fta)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, vals)
    con.commit()

@app.command()
def watchlist():
    """Show the current watchlist from players.yaml"""
    cfg = load_cfg()
    table = Table(title="Watchlist", box=box.SIMPLE_HEAVY)
    table.add_column("Player")
    table.add_column("Team")
    table.add_column("Season")
    for p in cfg["players"]:
        table.add_row(p["name"], p["team"], cfg.get("season",""))
    console.print(table)

@app.command()
def add(name: str, team: str):
    """Add a player to players.yaml"""
    cfg = load_cfg()
    cfg.setdefault("players", []).append({"name": name, "team": team})
    with open(CFG_PATH, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
    console.print(f"Added {name} ({team}) to watchlist.")

@app.command()
def remove(name: str, team: Optional[str] = typer.Option(None, help="Optional team filter")):
    """Remove a player from players.yaml"""
    cfg = load_cfg()
    before = len(cfg["players"])
    cfg["players"] = [p for p in cfg["players"] if not (p["name"].lower()==name.lower() and (team is None or p["team"].lower()==team.lower()))]
    with open(CFG_PATH, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
    console.print(f"Removed {before - len(cfg['players'])} entries.")

@app.command()
def sync():
    """Pull schedules + box scores for all players and store in SQLite"""
    cfg = load_cfg()
    season = cfg.get("season","2025-26")
    division = cfg.get("division","mens")
    con, cur = db()
    index = schools_index()

    # group players by team
    team_map = {}
    for p in cfg["players"]:
        team = p["team"]
        team_map.setdefault(team, []).append(p["name"])

    for team, names in team_map.items():
        slug = team_to_slug(team, index)
        if not slug:
            console.print(f"[yellow]Could not resolve team slug for {team}[/]")
            continue
        tid = save_team(cur, con, team, slug)
        sched = team_schedule(slug, season, division) or {}
        games = []
        for g in (sched.get("games") or []):
            gid = str(g.get("gameID") or g.get("gameId") or g.get("id") or "")
            if not gid: 
                continue
            games.append({
                "game_id": gid,
                "date": g.get("startDate") or g.get("date") or "",
                "home": (g.get("home",{}) or {}).get("names",{}).get("short",""),
                "away": (g.get("away",{}) or {}).get("names",{}).get("short",""),
                "status": g.get("gameState") or g.get("status",""),
            })
        for ge in games:
            upsert_game(cur, con, ge)
            try:
                box = game_box(ge["game_id"])
            except Exception as e:
                console.print(f"[yellow]Boxscore error {ge['game_id']}: {e}[/]")
                continue
            for side in ("home","away"):
                roster = (box.get(side, {}) or {}).get("players", []) or []
                for row in roster:
                    nm = (row.get("name") or "").strip()
                    if any(nm.lower()==want.lower() for want in names):
                        pid = str(row.get("personId") or row.get("id") or f"{nm}|{tid}")
                        upsert_player(cur, con, nm, tid, pid)
                        upsert_line(cur, con, pid, ge["game_id"], row)
            time.sleep(0.2)
    console.print(f"[green]Sync complete.[/] DB saved at {DB_PATH.resolve()}")

@app.command("stats")
def stats_command():
    import sqlite3
    from pathlib import Path

    try:
        from tabulate import tabulate
        _has_tab = True
    except Exception:
        _has_tab = False

    # use same DB_PATH the script defines at top if present, else fallback
    try:
        db_path = str(DB_PATH)
    except NameError:
        db_path = "cbb_tracker.sqlite"

    con = sqlite3.connect(db_path)

    rows = list(con.execute("""
    SELECT p.player_name AS Player,
           'Duke'        AS Team,
           ROUND(AVG(s.pts),1) AS PPG,
           ROUND(AVG(s.reb),1) AS RPG,
           ROUND(AVG(s.ast),1) AS APG,
           ROUND(AVG(s.stl),1) AS SPG,
           ROUND(AVG(s.blk),1) AS BPG,
           ROUND(AVG(s.tov),1) AS TOV
    FROM player_game_stats s
    JOIN players p ON p.player_id = s.player_id
    JOIN games   g ON g.game_id   = s.game_id
    WHERE p.team_id = '150'
      AND g.season_year = 2026
      AND lower(g.status) IN ('post','final','status_final')
    GROUP BY p.player_id
    ORDER BY PPG DESC;
    """))

    headers = ["Player","Team","PPG","RPG","APG","SPG","BPG","TOV"]

    if _has_tab and rows:
        print(tabulate(rows, headers=headers, tablefmt="simple_grid"))
    else:
        # fallback
        print("{:<20} {:<6} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5}".format(*headers))
        for r in rows:
            print("{:<20} {:<6} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5}".format(*r))

    con, _ = db()
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
    if df.empty:
        console.print("[yellow]No stats yet. Run: python cbb_tracker.py sync[/]")
        raise typer.Exit()
    table = Table(title="Season Averages", box=box.SIMPLE_HEAVY)
    for col in df.columns: table.add_column(col)
    for _, row in df.iterrows():
        table.add_row(*[str(x) for x in row.values.tolist()])
    console.print(table)

@app.command()
def schedule(team: str):
    """Show a team's upcoming games (per the API)"""
    cfg = load_cfg()
    season = cfg.get("season","2025-26")
    division = cfg.get("division","mens")
    idx = schools_index()
    slug = team_to_slug(team, idx)
    if not slug:
        console.print(f"[yellow]Could not resolve team slug for {team}[/]")
        raise typer.Exit()
    sched = team_schedule(slug, season, division) or {}
    games = sched.get("games") or []
    table = Table(title=f"{team} schedule ({season})", box=box.SIMPLE_HEAVY)
    table.add_column("Date"); table.add_column("Away"); table.add_column("Home"); table.add_column("Status")
    for g in games:
        table.add_row(g.get("startDate") or g.get("date",""),
                      (g.get("away",{}) or {}).get("names",{}).get("short",""),
                      (g.get("home",{}) or {}).get("names",{}).get("short",""),
                      g.get("gameState") or g.get("status",""))
    console.print(table)



from typing import List, Optional
import json, csv, sqlite3, typer





@app.command("player-boxscores")
def player_boxscores(
    names: list[str] = typer.Argument(None, help="Player names (exact or case-insensitive)."),
    file: str | None = typer.Option(None, "--file", "-f", help="Path to file (one name per line, CSV 'name' column, or JSON list)."),
    season: str = typer.Option("2025-26", help="Season like '2025-26' (uses end year internally)."),
    last: int = typer.Option(5, help="Show last N games per player."),
    team: str | None = typer.Option(None, help="Optional team_id to disambiguate (e.g., '150' for Duke).")
):
    import sqlite3, json, csv, re
    from pathlib import Path

    # Load names
    loaded = []
    if file:
        fp = Path(file)
        if not fp.exists():
            typer.echo(f"File not found: {file}")
            raise typer.Exit(1)
        if file.lower().endswith(".json"):
            loaded = json.loads(fp.read_text())
        elif file.lower().endswith(".csv"):
            with fp.open(newline="") as f:
                for row in csv.DictReader(f):
                    if "name" in row and row["name"].strip():
                        loaded.append(row["name"].strip())
        else:
            for line in fp.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    loaded.append(line)

    base = (list(names) if names else []) + loaded
    want = [n.strip() for n in base if isinstance(n, str) and n.strip()]
    if not want:
        typer.echo("No valid names provided. Pass names as args or use --file.")
        raise typer.Exit(1)

    # season → year
    m = re.match(r"^\s*(\d{4})\s*-\s*(\d{2})\s*$", season or "")
    if m:
        season_year = int(m.group(1)) + 1
    else:
        m2 = re.match(r"^\s*(\d{4})\s*$", season or "")
        season_year = int(m2.group(1)) if m2 else 2026

    try:
        db_path = str(DB_PATH)
    except NameError:
        db_path = "cbb_tracker.sqlite"

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    def resolve(name: str):
        exact = list(con.execute(
            "SELECT player_id, player_name, team_id FROM players WHERE lower(player_name)=lower(?)", (name,)
        ))
        if not exact:
            exact = list(con.execute(
                "SELECT player_id, player_name, team_id FROM players WHERE lower(player_name) LIKE lower(?)", (f"%{name}%",)
            ))
        if team:
            exact = [r for r in exact if str(r["team_id"]) == str(team)]
        return exact

    SQL = '''
    SELECT g.date, g.home_team, g.away_team,
           s.minutes, s.pts, s.reb, s.ast, s.stl, s.blk,
           s.fgm, s.fga, s.tpm, s.tpa, s.ftm, s.fta
    FROM player_game_stats s
    JOIN games g ON g.game_id = s.game_id
    WHERE s.player_id = ? AND g.season_year = ?
      AND lower(g.status) IN ('post','final','status_final')
    ORDER BY g.date DESC LIMIT ?;
    '''

    def print_table(title, rows):
        try:
            from tabulate import tabulate
            data = []
            for r in rows:
                matchup = f"{r['away_team']} @ {r['home_team']}"
                data.append([
                    r['date'][:10], matchup, r['minutes'], r['pts'], r['reb'], r['ast'],
                    r['stl'], r['blk'], r['fgm'], r['fga'], r['tpm'], r['tpa'], r['ftm'], r['fta']
                ])
            print(f"\n{title}")
            if data:
                print(tabulate(data, headers=['DATE','MATCHUP','MIN','PTS','REB','AST','STL','BLK','FGM','FGA','3PM','3PA','FTM','FTA'], tablefmt='simple_grid'))
            else:
                print('(no games)\n')
        except Exception:
            print(title)
            for r in rows:
                print(r)

    for name in want:
        matches = resolve(name)
        if not matches:
            print(f"\n[name not found] {name}")
            continue
        for m in matches:
            pid, pname, tid = m["player_id"], m["player_name"], m["team_id"]
            trow = con.execute("SELECT team_name FROM teams WHERE team_id=?", (tid,)).fetchone()
            team_disp = trow["team_name"] if trow else str(tid)
            rows = list(con.execute(SQL, (pid, season_year, last)))
            print_table(f"{pname} — {team_disp} (last {len(rows)} games)", rows)

if __name__ == "__main__":
    app()