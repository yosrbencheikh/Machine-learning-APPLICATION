from flask import Flask,render_template,url_for,request
import pandas as pd 
import numpy as np 
import pickle
import csv
from preprocessing import PreProcessing
from sklearn.feature_extraction.text import CountVectorizer
from nltk import word_tokenize
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras import backend as K



app = Flask(__name__,template_folder='templates')
@app.route('/', methods=['GET','POST'])

def home():
	return render_template('home.html')





@app.route('/predict',  methods=['GET','POST'])
def predict():
    #import translated dataframe
    df = pd.read_pickle("/home/yosr/prediction app/translation/TraintopicsTranslated.pkl")

    #resave classes (features) in vectorizer so we can inverse the results later and get the class name
    vectorizer = CountVectorizer(tokenizer =  lambda x: x.split(","))
    y = vectorizer.fit_transform(df['categories']).toarray()

    #importing model by using pickle
    with open('Lstm_model70.pickle', 'rb') as f:
        clf=pickle.load(f)


    if request.method == 'POST':
        
        message = request.form['message']
        print(type(message))
        text = [message]
        textDf = pd.DataFrame([message])
        print ('preprocessing...')
        textDf[0] = textDf[0].map(lambda com : PreProcessing(com))
        print(textDf)
        
        #padding : text representation
        print ('padding ...')
        vect = Tokenizer()
        #vect.fit_on_texts(df['translated_text'])
        vect.fit_on_texts(textDf[0])
        vocab_size = len(vect.word_index) +1
        encoded_docs = vect.texts_to_sequences(textDf[0])
        padded_sequences = pad_sequences(encoded_docs, maxlen= vocab_size, padding='post')
        print(padded_sequences)


        import tensorflow as tf
        graph = tf.get_default_graph()  # Function: Get the current default calculation chart. 
        with graph.as_default():
            print( 'predicting ...')
            predictions = clf.predict(padded_sequences)
            pred = predictions.copy()
            threshold = 0.1
            pred[predictions>= threshold] = 1
            pred[predictions<= threshold] = 0
            pred_classes = vectorizer.inverse_transform(pred)




       
        for i in pred_classes:
            print(i)
        K.clear_session()

    return render_template('predict.html', prediction = i)



           

if __name__ == '__main__':
    app.run(debug = True)

