import datetime
import os
import pickle as pkl
from pymongo import MongoClient, errors
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
                                # remove any potential \n after the title
                                title = f.readline().strip()
                                content = f.read()

                            yield {
                                "category": category,
                                "title": title,
                                "content": content,
                            }


# the next two generators have to perform two separate reads to be used
# but there needs to be a way to "lock" the DB until both are done to keep from
# inconsistent states throwing the _id->content pairs off


def content_generator(collection):
    results = collection.find({}, projection={"_id": 0, "content": 1})
    for r in results:
        yield r["content"]


def id_generator(collection):
    results = collection.find({}, projection={"_id": 1})
    for r in results:
        yield r["_id"]


def doc_generator(model, preprocessor, contents, disable, n_process, batch_size):
    nlp = spacy.load(model)
    nlp.add_pipe(preprocessor)
    return nlp.pipe(
        contents, disable=disable, n_process=n_process, batch_size=batch_size
    )


class PreprocessPipeline:
    def __init__(self, mongodb_collection):
        self.mongodb_collection = mongodb_collection

    def filesystem_load(self, data_dir):
        collection = self.mongodb_collection
        file_data = file_generator(data_dir=data_dir)
        collection.create_index("title", name="avoid_dups", unique=True)
        try:
            result = collection.insert_many(file_data, ordered=False)
        except errors.BulkWriteError:
            pass
        return self

    def spacy_doc_load(self, model, preprocessor, disable, n_process, batch_size):
        collection = self.mongodb_collection
        contents = content_generator(collection=collection)
        # I think, technically, a lock should be placed on the DB since the DB
        # reads could happen under diff states (document added btwn reads)
        # but for my purposes right now there is no need to worry about it
        ids = id_generator(collection=collection)
        docs = doc_generator(
            model=model,
            preprocessor=preprocessor,
            contents=contents,
            disable=disable,
            n_process=n_process,
            batch_size=batch_size,
        )
        preprocessor_bytestr = pkl.dumps(preprocessor)
        date = datetime.datetime.today()

        for _id, doc in zip(ids, docs):
            doc_bytestr = doc.to_bytes()
            collection.update_one(
                {"_id": _id},
                {
                    "$push": {
                        "spacy_doc": {
                            "date": date,
                            "model": model,
                            "preprocessor_bytestr": preprocessor_bytestr,
                            "doc_bytestr": doc_bytestr,
                            "doc_sample": doc[:5].text,
                        }
                    }
                },
            )
        return


def preprocessing_pipeline_cli():
    import argparse

    preprocessor_dict = {"spaces_punct_stopwords_lemma": preprocess_tokens}

    parser = argparse.ArgumentParser(
        description="Filesystem -> preprocessed spaCy Doc pipeline"
    )

    parser.add_argument(
        "model", type=str, help="a spaCy model capable of loading (en_core_web_lg)"
    )
    parser.add_argument("preprocessor", type=str, help=f"{preprocessor_dict.keys()}")
    parser.add_argument(
        "--disable",
        type=str,
        nargs="*",
        default=[],
        help="spaCy pipeline components to disable: ner, tagger, parser",
    )
    parser.add_argument(
        "--n-process",
        type=int,
        default=1,
        help="number of processors to run the Doc creation on",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="buffer size for Doc creation"
    )
    parser.add_argument(
        "--close-fs-valve",
        action="store_true",
        help="flag this to avoid read from filesystem and use what is already stored in database",
    )
    parser.add_argument(
        "--dev", action="store_true", help="flag this to use dev env (DB/collection)"
    )

    args = parser.parse_args()

    client = MongoClient()

    if args.dev:
        # grab the dev env
        db = client.bbcDev
    else:
        # grab the production
        db = client.bbc

    collection = db.article

    pipeline = PreprocessPipeline(mongodb_collection=collection)

    if args.close_fs_valve:  # change preprocessor implementation
        print("--X--[raw]--[spacy]--... \n>>>filesystem valve closed")
        print("Creating spaCy docs and loading into DB...")
        pipeline.spacy_doc_load(
            model=args.model,
            preprocessor=preprocessor_dict[args.preprocessor],
            disable=args.disable,
            n_process=args.n_process,
            batch_size=args.batch_size,
        )
        print("Creation and upload complete.")


if __name__ == "__main__":
    # from pymongo import MongoClient

    # client = MongoClient()
    # db = client.bbcDev
    # collection = db.article

    # pipeline = PreprocessPipeline(collection)
    # # pipeline.filesystem_load("data")
    # pipeline.spacy_doc_load(
    #     model="en_core_web_sm",
    #     preprocessor=preprocess_tokens,
    #     disable=["ner", "parser", "tagger"],
    #     n_process=4,
    #     batch_size=50,
    # )
    preprocessing_pipeline_cli()
