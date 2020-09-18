import datetime
from bson import ObjectId
from pymongo import MongoClient

client = MongoClient()
db = client.bbcDev
collection = db.article
model_date = datetime.datetime.fromisoformat("2020-09-16T15:19:03.553+00:00")
# print(model_date)

_id = ObjectId("5f5d050e2cc0784013d43d46")
result = collection.find_one({"spacy_doc.date": model_date}, {"spacy_doc.date": 1})

print(result)

# print(datetime.datetime(2020, 9, 12, 14, 8, 23, 670000).isoformat())