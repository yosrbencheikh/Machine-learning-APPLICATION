from nltk import word_tokenize
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.preprocessing import sequence


def pad_sequences(text):
    vect = Tokenizer()
    vect.fit_on_texts(text)
    vocab_size = len(vect.word_index) +1
    encoded_docs = vect.texts_to_sequences(text)
    max_length = vocab_size
    padded_docs = pad_sequences(encoded_docs)
    #padded_docs = pad_sequences(encoded_docs, maxlen= max_length, padding = 'post')
    return(padded_docs)


