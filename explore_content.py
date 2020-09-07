from pymongo import MongoClient
import spacy

nlp = spacy.load("en_core_web_md")


def extract_content(client):
    db = client.bbc
    collection = db.article
    result_cursor = collection.find({}, projection={"content": 1})
    return [(r["_id"], r["content"]) for r in result_cursor]


client = MongoClient()
contents = extract_content(client=client)
# print(contents[0])


# giant_doc_text = " +++ ".join(contents)
# print(giant_doc)

# docs = [nlp(c) for c in contents] # this takes a long time
# suggestion is to process as one doc and then split into spans
# spans can still be compared with .similarity as docs can be (and tokens)

# giant_doc = nlp(giant_doc_text) # this exceeds the max length of 1000000