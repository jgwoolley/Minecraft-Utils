import sqlite3, pathlib, datetime

sessions_select_sql = '''
SELECT
	source_id,
	MINECRAFT_SERVER_SESSIONS.player, 
	SUM(MINECRAFT_SERVER_SESSIONS.duration) as duration,
	MAX(MINECRAFT_SERVER_SESSIONS.left_time) as left_time,
	MIN(MINECRAFT_SERVER_SESSIONS.login_time) as login_time
FROM
	MINECRAFT_SERVER_SESSIONS
WHERE source_id = (
	SELECT MAX(source_id) FROM MINECRAFT_SERVER_SESSIONS
)
GROUP BY 
	MINECRAFT_SERVER_SESSIONS.player
ORDER BY duration DESC
'''

def format_seconds(total_seconds: float):
    days, remainder = divmod(total_seconds, 86400)
    hours, seconds = divmod(remainder, 3600)

    result = ""
    if days == 1:
        result+=f"{int(days)} day"
    else:
        result+=f"{int(days)} days"

    if hours == 1:
        result+=f" {int(hours)} hour"
    elif hours > 1:
        result+=f" {int(hours)} hours"

    return result

def main():
    database = "results.db"
    output_path = pathlib.Path("out_report.html")

    with sqlite3.connect(database=database) as con:
        cur = con.cursor()
        result = cur.execute(sessions_select_sql)

        with output_path.open("w") as file:
            file.write('''
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Minecraft-Utils</title>
            </head>
            <body>
                <main>
                    <h1>Welcome to Minecraft-Utils</h1>
                    <h3>Login Times</h3>
                    <table border=1 frame=BOX rules=all>
                        <tr>
                            <th>Player</th>
                            <th>Total Play Time</th>
                            <th>First Login</th>
                            <th>Last Login</th>
                            <th>Days Since First Login</th>
                        </tr>
            ''')
            for row in result:
                source_id, player, duration, left_time, login_time = row
                duration = float(duration)
                left_time = datetime.datetime.fromisoformat(left_time)
                login_time = datetime.datetime.fromisoformat(login_time)
                seconds_since_first = (left_time - login_time).total_seconds()

                file.write("<tr>")
                file.write(f"<td>{player}</td>\n")
                file.write(f"<td title=\"{duration} seconds\">{format_seconds(duration)}</td>\n")
                file.write(f"<td title=\"{login_time}\">{login_time:%Y-%m-%d}</td>\n")
                file.write(f"<td title=\"{left_time}\">{left_time:%Y-%m-%d}</td>\n")
                file.write(f"<td title=\"{seconds_since_first} seconds\">{format_seconds(seconds_since_first)}</td>\n")
                file.write("</tr>\n")
            
            file.write('''
                        </table>  
                    </main>
                </body>
            </html>
            ''')

if __name__ == "__main__":
    main()