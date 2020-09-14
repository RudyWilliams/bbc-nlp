# import pickle as pkl
# from pymongo import MongoClient
import spacy

# from preprocessing_pipe import *

# # so the best plan is to probably create
# # a preprocessing functions script and load in all like so. Then the
# # function will be in the namespace when unpickling

# client = MongoClient()
# db = client.bbcDev
# collection = db.article

# result = collection.find_one()
# model = result["spacy_doc"][0]["model"]
# pbytestr = result["spacy_doc"][0]["preprocessor_bytestr"]

# nlp = spacy.load(model)
# doc = nlp("This dog can run quickly. When running he is a very fast dog.")
# preprocessor = pkl.loads(pbytestr)

# clean_doc = preprocessor(doc)

# print(clean_doc)
nlp = spacy.load("en_core_web_lg")