def func1():
    for i in range(10):
        for j in range(3):
            if j != 0:
                yield (i, j)


def func2():
    for obj in func1():
        yield obj[1]


print(len(list(func2())))


def func3():
    for c in [
        "this is a sentence",
        "this is another sentence",
        "I like pizza and pasta.",
    ] * 100:
        yield c


# import spacy

# nlp = spacy.load("en_core_web_sm")
# docs = nlp.pipe(func3())
# print(list(docs))
