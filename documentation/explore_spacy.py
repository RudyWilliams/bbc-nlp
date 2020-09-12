import spacy
from spacy.lang.en import English

nlp = English()
doc = nlp("This is a sentence with number five (5).")

for token in doc:
    print(token)
print("=====")

print(doc[0])  ## This
print(doc[1:3])  ## prints tokens 1 & 2 (not 0 or 3 or 4)
print("=====")

print(f"is alpha: {[t.is_alpha for t in doc]}")
print(f"is punctuation: {[t.is_punct for t in doc]}")
print(
    f"like number: {[t.like_num for t in doc]}"
)  ## see five and 5 are both picked up as like number
print("=====")

# creating nlp object with english models loaded in
nlp = spacy.load("en_core_web_sm")

doc = nlp("The football player ran down the sideline.")
for token in doc:
    print(token.text, ":", token.pos_)
print("=====")

doc = nlp(
    "The store, Target, sells apples and other fruits in the United States (US). Rudy Williams (Mr. Williams) buys them when he has $5."
)
for ent in doc.ents:
    print(ent.text, ":", ent.label_)

print(nlp.vocab.strings["rudy"])
print(nlp.vocab.strings["Rudy"])
print(nlp.vocab.strings[6053231006704361918])
