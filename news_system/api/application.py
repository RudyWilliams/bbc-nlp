from flask import Flask, jsonify, request
from markupsafe import escape
from pymongo import MongoClient
from pymongo.collation import Collation

application = Flask(__name__)
client = MongoClient()
db = client.bbcTest
collection = db.article


@application.route("/")
def api_hello():
    return "Hello from the BBC News Similarity API"


@application.route("/article", methods=["GET"])
def respond_to_article():
    args = request.args
    title = escape(args.get("title"))
    n_similar = int(escape(args.get("nsimilar")))

    collation = Collation(locale="en", strength=1)

    result = collection.aggregate(
        pipeline=[
            {"$match": {"title": title}},
            {
                "$project": {
                    "_id": 0,
                    "spacy_doc": 0,
                    "similarities.sim_array.other_id": 0,
                }
            },  # will need other_id but just checking response
            {"$unwind": "$similarities.sim_array"},
            {"$sort": {"similarities.sim_array.sim": -1}},
            {"$limit": n_similar},
        ],
        collation=collation,
    )
    return jsonify(list(result))


if __name__ == "__main__":
    application.run()