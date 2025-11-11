import re, pathlib, textwrap

p = pathlib.Path("cbb_tracker.py")
src = p.read_text()

# Remove any old player-boxscores block
src = re.sub(r"@app\.command\(['\"]player-boxscores['\"]\)[\s\S]*?(?=\n@app\.command|\nif __name__|$)", "", src)

new_func = textwrap.dedent("""
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
    m = re.match(r"^\\s*(\\d{4})\\s*-\\s*(\\d{2})\\s*$", season or "")
    if m:
        season_year = int(m.group(1)) + 1
    else:
        m2 = re.match(r"^\\s*(\\d{4})\\s*$", season or "")
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
            print(f"\\n{title}")
            if data:
                print(tabulate(data, headers=['DATE','MATCHUP','MIN','PTS','REB','AST','STL','BLK','FGM','FGA','3PM','3PA','FTM','FTA'], tablefmt='simple_grid'))
            else:
                print('(no games)\\n')
        except Exception:
            print(title)
            for r in rows:
                print(r)

    for name in want:
        matches = resolve(name)
        if not matches:
            print(f"\\n[name not found] {name}")
            continue
        for m in matches:
            pid, pname, tid = m["player_id"], m["player_name"], m["team_id"]
            trow = con.execute("SELECT team_name FROM teams WHERE team_id=?", (tid,)).fetchone()
            team_disp = trow["team_name"] if trow else str(tid)
            rows = list(con.execute(SQL, (pid, season_year, last)))
            print_table(f"{pname} — {team_disp} (last {len(rows)} games)", rows)
""")

insert_at = src.rfind("if __name__")
if insert_at == -1:
    src += "\n\n" + new_func + "\n"
else:
    src = src[:insert_at] + "\n\n" + new_func + "\n" + src[insert_at:]

p.write_text(src)
print("✅ player-boxscores replaced successfully.")
