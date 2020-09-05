import os
import sqlite3

data_dir = "data"


# create database
conn = sqlite3.connect("bbc.db")
c = conn.cursor()
c.execute(
    "CREATE TABLE IF NOT EXISTS article (category text, title text, content text)"
)
conn.commit()

values = []

with os.scandir(data_dir) as it1:
    for t1 in it1:
        # store
        category = t1.name

        if t1.is_dir():
            with os.scandir(t1.path) as it2:
                for t2 in it2:
                    if t2.is_file():
                        with open(t2.path, "r") as f:
                            title = f.readline()
                            content = f.read()
                            values.append((category, title, content))

c.executemany("INSERT INTO article VALUES (?, ?, ?)", values)
conn.commit()
conn.close()