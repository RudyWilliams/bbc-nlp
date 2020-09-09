import datetime
import pickle as pkl
from pymongo import MongoClient
import spacy

model = "en_core_web_lg"
nlp = spacy.load(model)


def extract_content(client):
    db = client.bbc
    collection = db.article
    result_cursor = collection.find({}, projection={"content": 1})
    return [(r["_id"], r["content"]) for r in result_cursor]


def preprocess_tokens(doc):
    """
    1. lemmatize text
    2. remove stopwords and punctuation and newline symbols
    """
    filtered_tokens = [
        t.lemma_ for t in doc if not (t.is_punct or t.is_stop or t.is_space)
    ]
    return spacy.tokens.Doc(vocab=doc.vocab, words=filtered_tokens)


client = MongoClient()
id_contents = extract_content(client=client)
contents = [idc[1] for idc in id_contents]

preprocess_bytestr = pkl.dumps(preprocess_tokens)
print(preprocess_bytestr)
nlp.add_pipe(preprocess_tokens)
docs = nlp.pipe(contents, disable=["ner", "parser", "tagger"])

ids = [idc[0] for idc in id_contents]

ids_docs = zip(ids, docs)

db = client.bbc
collection = db.article
today = datetime.date.today()

print("updating mongo")
for id_doc in ids_docs:
    _id = id_doc[0]
    doc = id_doc[1]
    doc_bytestr = doc.to_bytes()
    collection.update_one(
        {"_id": _id},
        {
            "$set": {
                f"spacy_doc.{today}.doc_bytestr": doc_bytestr,
                f"spacy_doc.{today}.preprocess_bytestr": preprocess_bytestr,
                f"spacy_doc.{today}.model": model,
            }
        },
    )
print("done")
