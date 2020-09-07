from pymongo import MongoClient
import spacy

nlp = spacy.load("en_core_web_md")


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


nlp.add_pipe(preprocess_tokens)
docs = list(nlp.pipe(contents, disable=["ner", "parser", "tagger"])


doc1 = docs[0]
for t in doc1:
    print(t)
