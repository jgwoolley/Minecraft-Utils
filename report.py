import sqlite3, pathlib, datetime

sessions_select_sql = '''
SELECT
	source_id,
	MINECRAFT_SERVER_SESSIONS.player, 
	SUM(MINECRAFT_SERVER_SESSIONS.duration) as duration
FROM
	MINECRAFT_SERVER_SESSIONS
WHERE source_id = (
	SELECT MAX(source_id) FROM MINECRAFT_SERVER_SESSIONS
)
GROUP BY 
	MINECRAFT_SERVER_SESSIONS.player
ORDER BY duration DESC
'''

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
                    <ol>
            ''')
            for row in result:
                duration = datetime.timedelta(seconds=float(row[2]))
                file.write(f"<li title=\"{row[2]} seconds\">{row[1]}: {duration}</li>\n")

            file.write('''
                <ol>  
                    </main>
                </body>
            </html>
            ''')

if __name__ == "__main__":
    main()