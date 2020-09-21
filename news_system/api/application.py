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
    title = escape(request.args.get("title"))
    collation = Collation(locale="en", strength=1)
    result = collection.find_one(
        {"title": title},
        projection={"_id": 0, "title": 1, "category": 1, "content": 1},
        collation=collation,
    )
    return jsonify(result)


if __name__ == "__main__":
    application.run()