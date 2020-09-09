import os
import pickle as pkl
from pymongo import MongoClient
import spacy
from spacy.tokens import Doc


def preprocess_tokens(doc):
    """
    1. lemmatize text
    2. remove stopwords and punctuation and newline symbols
    """
    filtered_tokens = [
        t.lemma_ for t in doc if not (t.is_punct or t.is_stop or t.is_space)
    ]
    return spacy.tokens.Doc(vocab=doc.vocab, words=filtered_tokens)


def file_generator(data_dir):

    with os.scandir(data_dir) as it1:
        for t1 in it1:
            if t1.is_dir():
                category = t1.name

                with os.scandir(t1.path) as it2:
                    for t2 in it2:
                        if t2.is_file():
                            with open(t2.path, "r") as f:
                                title = f.readline()
                                content = f.read()

                            yield (category, title, content)


def content_generator(data_dir):
    files = file_generator(data_dir)
    for f in files:
        content = f[-1]
        yield content


def spacy_docs_generator(
    contents, model, preprocessing_func, n_process=1, batch_size=1, disable=None
):
    if disable is None:
        disable = []
    nlp = spacy.load(model)
    nlp.add_pipe(preprocessing_func)
    return nlp.pipe(
        contents, batch_size=batch_size, n_process=n_process, disable=disable
    )


if __name__ == "__main__":
    contents = content_generator("data")

    docs = spacy_docs_generator(
        contents,
        "en_core_web_sm",
        preprocess_tokens,
        disable=["ner", "parser", "tagger"],
        batch_size=50,
        n_process=1,
    )
    docs_list = list(docs)
