# from spacy.lang.en import stop_words
import spacy

text = "\nThis is a text.\n\nThe internet connection is running lower than usual. Don't be upset with me about it"

nlp = spacy.load("en_core_web_sm")
doc = nlp(text)


cleaned = [t.lemma_ for t in doc if not (t.is_space or t.is_stop or t.is_punct)]
clean_doc = spacy.tokens.Doc(nlp.vocab, words=cleaned)
print(clean_doc.vocab == nlp.vocab)
for t in clean_doc:
    print(t.is_alpha)