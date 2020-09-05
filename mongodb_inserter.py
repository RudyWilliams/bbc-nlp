import os
from pymongo import MongoClient

data_dir = "data"

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
                            values.append(
                                {
                                    "title": title,
                                    "category": category,
                                    "content": content,
                                }
                            )

client = MongoClient()
bbc_db = client.bbc
article_collection = bbc_db.article

result = article_collection.insert_many(values)