import pycountry, numpy as np, pandas as pd, langdetect, re, langid
import json, requests, time, ast
from datetime import datetime
from config import config
import os

JSON_HEADER = {"Content-Type": "application/json"}
YT8M_LOOKUP_URL = "http://data.yt8m.org/2/j/i/{first_2}/{video_id}.js"

class CommonHelper():
    """ 
    A helper class for common operations
    """

    
    @staticmethod
    def to_json(obj):
        """ 
        The function to convert the given obj to JSON.
        
        Parameters: 
            obj (obj): The object to be converted to JSON
        Returns: 
            JSON (str): The JSON text of the given obj
        """
        json_text = json.dumps(obj)
        return json.loads(json_text)

    @staticmethod
    def get_request(url, json=True) :
        if json :
            result = requests.get(url, headers=JSON_HEADER)
        else : 
            result = requests.get(url)
        if result.status_code == 200:
            if json :
                return json.loads(result.text)
            else :
                return result.text
        elif result.status_code == 429:
            time.sleep(30)
            CommonHelper.get_request(url, json)
        else:
            return []

    @staticmethod
    def lookup_id(video_id) :
        result = CommonHelper.get_request(YT8M_LOOKUP_URL.format(first_2 = video_id[:2], video_id = video_id), json=False)
        if result == [] :
            return None
        ls_split = result.split('"')
        return ls_split[len(ls_split)-2]


    @staticmethod
    def chunks(it, size):
        """ 
        The function to return the chunks of the given 'it' by the given size.
        
        Parameters: 
            it (list): The list for the chunk
            size (int): The size of each chunk
        Returns: 
            chunks (iter): The iter of chunks from the given list
        """
        from itertools import islice
        if not it : 
            return None
        try:
            it = iter(it)
        except TypeError :
            return None
        return iter(lambda: tuple(islice(it, size)), ())
    
    
    @staticmethod
    def get_country_code(country_name):
        """ 
        The function to return the country code of the given country name.
        
        Parameters: 
            country_name (str): The country name
        Returns: 
            country_code (str(2)): The country code of the given country name
        """
        if country_name.lower() == 'ivory coast':
            return 'CI'
        elif country_name.lower() == 'macedonia, the former yugoslav republic of':
            return 'MK'
        elif country_name.lower() == 'cape verde':
            return 'CV'
        elif country_name.lower() == 'reunion':
            return 'RE'
        elif country_name.lower() == 'barthelemy':
            return 'BL'
        else:
            try:
                return pycountry.countries.lookup(country_name).alpha_2
            except:
                return None
            
    
    @staticmethod
    def get_country_name(country_code):
        """ 
        The function to return the country name of the given country code.
        
        Parameters: 
            country_code (str(2)): The country code
        Returns: 
            country_name (str): The country name of the country code
        """
        try:
            return pycountry.countries.lookup(country_code).name
        except:
            return "Unspecified Country"
        
        
    @staticmethod
    def get_country_code_dict():
        """ 
        The function to return all the 3-letter country codes and names
        with their 2-letter country code values.
        
        Parameters: 
            None
        Returns: 
            codes (dict(country code(str(3)) or country name(str): country code(str(2)))):
                                                    The dictionary for all the countries
        """
        codes = {c.alpha_3.lower(): c.alpha_2.lower() for c in pycountry.countries} 
        names = {c.name.lower(): c.alpha_2.lower() for c in pycountry.countries}
        codes.update(names)
        return codes
    
    
    @staticmethod
    def get_country_codes():
        """ 
        The function to return all the 2-letter country codes.
        
        Parameters: 
            None
        Returns: 
            codes (list(str(2))): The country code list
        """
        return [c.alpha_2.lower() for c in pycountry.countries]
        
        
    @staticmethod
    def get_country_form_list():
        """ 
        The function to return all the pairs of 2-letter country code and its country name.
        
        Parameters: 
            None
        Returns: 
            country code and name pairs (list(pair(country code(str(2)), country name(str)))):
                                                        The country code and name pairs
        """
        return sorted([(c.alpha_2, c.name)for c in pycountry.countries], key=lambda country: country[1])
    
    
    @staticmethod
    def convert_int_data_type_as_proper_one(series, max_value):
        """ 
        The function to convert the given pandas.Series to the proper int data type based on the given max_value.
        
        Parameters: 
            series (pandas.Series): The series to convert its int data type
            max_value (int): The max value for the proper int data type
        Returns: 
            series (pandas.Series): The data type converted pandas.Series
        """
        if max_value < 255:
            return series.astype(np.uint8)
        elif max_value < 65535:
            return series.astype(np.uint16)
        elif max_value < 4294967295:
            return series.astype(np.uint32)
        else:
            return series    
                
    @staticmethod
    def load_pickle(path):
        """ 
        The function to return the given path(pickle)'s pandas.DataFrame.
        
        Parameters: 
            path (str): The file path(pickle) of the pandas.DataFrame
        Returns: 
            dataframe (pandas.DataFrame): The pandas.DataFrame of the path
        """
        import pickle
        try:
            with open(path, 'rb') as f:
                pickle_data = pickle.load(f)
        except UnicodeDecodeError as e:
            with open(path, 'rb') as f:
                pickle_data = pickle.load(f, encoding='latin1')
        except Exception as e:
            raise e
        return pickle_data    
        
    @staticmethod
    def clean_text(text):
        """ 
        The function to clean the text.
        
        Parameters: 
            text (str): The text to be cleaned
        Returns: 
            cleaned_text (str): The cleaned text
        """
        return ' '.join([t for t in re.split(r'[`/\n/\r\-=~!@#$%^&*()_+\[\]{};\'\\:"|<,./<>? ]', text) if t != ''])
    
    
    @staticmethod
    def remove_hashtag_and_url(text):
        """ 
        The function to remove hashtags and URLs from the text.
        
        Parameters: 
            text (str): The text for removing hashtags and URLs
        Returns: 
            cleaned_text (str): The cleaned text
        """
        return ' '.join(re.sub("(@[_.A-Za-z0-9]+)|(\w+:\/\/\S+)"," ",text).split()).strip()


    @staticmethod
    def detect_language(text):
        """ 
        The function to detect the given text's language.
        
        Parameters: 
            text (str): The text for the language detection
        Returns: 
            language code (str): The language code for the text
        """
        try:
            lang = langdetect.detect(text)
            if lang == 'unknown':
                return langid.classify(text)[0]
            else:
                return lang
        except:
            try:
                return langid.classify(text)[0]
            except:
                return None
