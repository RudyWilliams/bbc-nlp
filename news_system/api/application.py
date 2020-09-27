from flask import Flask, jsonify, request
from markupsafe import escape
from pymongo import MongoClient
from pymongo.collation import Collation

application = Flask(__name__)
client = MongoClient()
db = client.bbcDev
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

    content_result = list(
        collection.aggregate(
            pipeline=[
                {"$match": {"title": title}},
                {
                    "$project": {
                        "_id": 0,
                        "title": 1,
                        "category": 1,
                        "content": 1,
                        "similarities": 1,
                    }
                },
                {"$unwind": "$similarities.sim_array"},
                {"$sort": {"similarities.sim_array.sim": -1}},
                {"$limit": n_similar},
            ],
            collation=collation,
        )
    )  # breaking with my use of generators bc this should never be too large

    similars_oids = [r["similarities"]["sim_array"]["other_id"] for r in content_result]
    similars_result = list(
        collection.find({"_id": {"$in": similars_oids}}, {"_id": 0, "title": 1})
    )

    content_dict = content_result[0]  # just need one of the unwind returns
    _ = content_dict.pop("similarities", None)

    response = {"article": content_dict, "similars": similars_result}

    return jsonify(response)


if __name__ == "__main__":
    application.run()