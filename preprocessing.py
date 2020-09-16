import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from googletrans import Translator

#translation function
def translate(text):
    translator = Translator()
    return translator.translate(text, dest='en').text


#function to clean the word of any punctuation or special characters
def cleanPunc(sentence):
    cleaned = re.sub(r'[?|!|\'|"|#]',r'',sentence)
    cleaned = re.sub(r'[.|,|)|(|\|/]',r' ',cleaned)
    cleaned = cleaned.strip()
    cleaned = cleaned.replace("\n"," ")
    return cleaned

#function to remove digits just to make sure
def removeCharDigit(text):
    str='`1234567890-=~@#$%^&*()►■●•_+[!{;":\'><.,/?"}]'
    for w in text:
        if w in str:
            text=text.replace(w,'')
    return text


def keepAlpha(sentence):
    alpha_sent = ""
    for word in sentence.split():
        alpha_word = re.sub('[^a-z A-Z]+', ' ', word)
        alpha_sent += alpha_word
        alpha_sent += " "
    alpha_sent = alpha_sent.strip()
    return alpha_sent

#function to remove accented chars
import unicodedata
def removeAscendingChar(data):
    data=unicodedata.normalize('NFKD', data).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return data


#removing stopwords
from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))
stop_words.update(['zero','one','two','three','four','five','six','seven','eight','nine','ten','may','also','across','among','beside','however',
                   'yet','within','www','e','de','la','el','v','www','http','com','https'])
re_stop_words = re.compile(r"\b(" + "|".join(stop_words) + ")\\W", re.I)

def removeStopWords(sentence):
    global re_stop_words
    return re_stop_words.sub(" ", sentence)


#stemming
from nltk.stem.snowball import SnowballStemmer
stemmer = SnowballStemmer("english")
def stemming(sentence):
    stemSentence = ""
    for word in sentence.split():
        stem = stemmer.stem(word)
        stemSentence += stem
        stemSentence += " "
    stemSentence = stemSentence.strip()
    return stemSentence



#apply changes

#put all the unctions above into one global function
def PreProcessing(text):
    translator = Translator()
    text=removeAscendingChar(text)
    #print(text)
    text=cleanPunc(text)
    #print(text)
    text=keepAlpha(text)
    #print(text)
    text=removeCharDigit(text)
    text = text.lower()
    #text = translate(text)
    text = removeStopWords(text)
    text = stemming(text)
    return(text)


