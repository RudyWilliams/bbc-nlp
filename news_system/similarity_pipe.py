# the ml is really done internally as far as mapping words/docs to vectors
# we could perform clustering down the line to group categories
# so that the system does not need to perform similarity calculations
# for all document pairs
import datetime
import spacy
from spacy.tokens import Doc
import progressbar


class SimilarityLoader:
    def __init__(self, mongodb_collection):
        self.mongodb_collection = mongodb_collection

    def set_ids_full_set(self):
        results = self.mongodb_collection.find({}, ["_id"])
        self._ids_full_set = {r["_id"] for r in results}
        return self

    def initial_loader(self, model_date):
        model_datetime = datetime.datetime.fromisoformat(model_date)
        collection = self.mongodb_collection

        try:
            _ids_full_set = self._ids_full_set
        except AttributeError:
            self.set_ids_full_set()
            _ids_full_set = self._ids_full_set

        model = self._query_model_str(model_datetime=model_datetime)
        nlp = spacy.load(model)
        nlp_vocab = nlp.vocab

        widgets = [
            "Completed: ",
            progressbar.Counter("%(value)d"),
            " (",
            progressbar.Timer(),
            ")",
        ]
        bar = progressbar.ProgressBar(widgets=widgets)

        for i, _id in enumerate(_ids_full_set):

            result = self._query_spacy_doc(_id=_id, model_datetime=model_datetime)
            spacy_doc = self._convert_doc_bytestr(result=result, nlp_vocab=nlp_vocab)

            similarities = result.get("similarities", dict()).get("sim_array", [])
            already_calc_set = {s["other_id"] for s in similarities}
            need_calc_set = _ids_full_set - already_calc_set - {_id}

            new_sims = []
            for other_id in need_calc_set:
                other_result = self._query_spacy_doc(
                    _id=other_id, model_datetime=model_datetime
                )
                other_spacy_doc = self._convert_doc_bytestr(
                    result=other_result, nlp_vocab=nlp_vocab
                )

                sim = spacy_doc.similarity(other_spacy_doc)
                new_sims.append({"other_id": other_id, "sim": sim})
                self._push_single_sim(
                    _id=other_id,
                    model_datetime=model_datetime,
                    sim={"other_id": _id, "sim": sim},
                )

            self._push_new_sims(
                _id=_id, model_datetime=model_datetime, new_sims=new_sims
            )
            bar.update(i + 1)
        bar.finish()
        return

    def _query_spacy_doc(self, _id, model_datetime):
        result = self.mongodb_collection.find_one(
            {"_id": _id},
            {
                "similarities": 1,
                "spacy_doc": {"$elemMatch": {"date": model_datetime}},
            },
        )
        return result

    @staticmethod
    def _convert_doc_bytestr(result, nlp_vocab):
        spacy_doc_list = result.get("spacy_doc", [{"doc_bytestr": None}])
        spacy_doc_bytestr = spacy_doc_list[0]["doc_bytestr"]

        if spacy_doc_bytestr is None:
            raise ValueError(
                "Encountered an article w/o spaCy Doc for that model's date. Please run the preprocessing pipeline to create."
            )
        return Doc(nlp_vocab).from_bytes(spacy_doc_bytestr)

    def _query_model_str(self, model_datetime):
        return self.mongodb_collection.find_one(
            {"spacy_doc.date": model_datetime},
            {
                "_id": 0,
                "spacy_doc": {"$elemMatch": {"date": model_datetime}},
            },
        )["spacy_doc"][0]["model"]

    def _push_new_sims(self, _id, model_datetime, new_sims):
        self.mongodb_collection.update_one(
            {"_id": _id},
            {
                "$set": {"similarities.model_date": model_datetime},
                "$push": {"similarities.sim_array": {"$each": new_sims}},
            },
        )
        return

    def _push_single_sim(self, _id, model_datetime, sim):
        self.mongodb_collection.update_one(
            {"_id": _id},
            {
                "$set": {"similarities.model_date": model_datetime},
                "$push": {"similarities.sim_array": sim},
            },
        )
        return


def similarity_loader_cli():
    import argparse
    from pymongo import MongoClient

    parser = argparse.ArgumentParser(
        description="Calculate similarities for given model"
    )

    parser.add_argument(
        "model_date",
        type=str,
        help="The model date that acts as identifier stored in DB (in ISO format)",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="When flagged, run in development environment",
    )

    args = parser.parse_args()

    client = MongoClient()

    if args.dev:
        db = client.bbcDev
    else:
        db = client.bbc

    collection = db.article

    loader = SimilarityLoader(mongodb_collection=collection)
    loader.initial_loader(args.model_date)

    return


if __name__ == "__main__":
    from pymongo import MongoClient

    # client = MongoClient()
    # db = client.bbcTest
    # collection = db.article

    # simload = SimilarityLoader(collection)
    # simload.initial_loader("2020-09-16T15:19:03.553+00:00")
    # print(new_sims)
