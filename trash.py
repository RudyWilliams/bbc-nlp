from pymongo import MongoClient
from pymongo.collation import Collation

client = MongoClient()
db = client.bbcTest
collection = db.article

# collation = Collation(locale="en", strength=1)

title = "Ad sales boost Time Warner profit"
n_similar = 2

result = collection.aggregate(
    pipeline=[
        {"$match": {"title": title}},
        {"$project": {"_id": 0, "spacy_doc": 0}},
        {"$unwind": "$similarities.sim_array"},
        {"$sort": {"similarities.sim_array.sim": -1}},
        {"$limit": n_similar},
    ],
    # collation=collation, # takes collation arg but no collation index for this collection
)

for doc in result:
    print(doc["similarities"]["sim_array"]["sim"])

# print(collection.find_one({"title": title}))