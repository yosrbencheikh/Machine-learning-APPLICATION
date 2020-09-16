# -*- coding: utf-8 -*-
from sqlalchemy.dialects.postgresql import ENUM, ARRAY
from enum import Enum
from sqlalchemy import cast
import re

class ENUMType(Enum):
    
    @classmethod
    def ENUM(cls):
        return ENUM(*cls.list(), name=cls.__name__)
    
    @classmethod
    def list(cls):
        return [member.value for member in cls.__members__.values()]
    
    @classmethod
    def form_list(cls):
        return [(member.value, member.value) for member in cls.__members__.values()]
    
    def __str__(self):
        return self.value
    
    
class ArrayOfEnum(ARRAY):
    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(ArrayOfEnum, self).result_processor(dialect, coltype)

        def handle_raw_string(value):
            if value==None:
                return []
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",")

        def process(value):
            return super_rp(handle_raw_string(value))
        return process
    
class ContentAttribute(ENUMType) :
    ID = 'id'
    TITLE = 'title'
    DECRIPTION = 'decription'
    PUBLISHED_IN = 'published_in'
    THUMBNAIL_URL = 'thumbnail_url'
    URL = 'url'
    TAGS = 'tags'
    DEVICE = 'device'
    LANGUAGE = 'language'
    CHANNEL_ID = 'channel_id'
    CHANNEL_NAME = 'channel_name'
    CONTENT_VIEW_COUNT = 'view_count'
    CONTENT_LIKE_COUNT = 'content_like_count'
    CONTENT_AVERAGE_TIME = 'view_time_data'
    CONTENT_DISLIKE_COUNT = 'content_dislike_count'
    CONTENT_COMMENT_COUNT = 'content_comment_count'
    COMMENTS = 'comments'
    COMMENT_ID = 'comment_id'
    COMMENT_AUTHOR = 'comment_author'
    COMMENT_MESSAGE = 'comment_message'
    COMMENT_CREATION_DT = 'comment_creation_dt'
    COMMENT_LIKE_COUNT = 'comment_like_count'
    COMMENT_REPLY_COUNT = 'comment_reply_count'

    
class DatasetFieldDesignatedType(ENUMType):
    AGE = "age"
    AGE_RANGE = "age_range"
    COUNTRY_CODE = "country_code"
    CITY = "city"
    GENDER = "gender"
    OPTIONAL = "optional"
    CONTENT = "content"
    INSTANCES = "instances"
    ATTRIBUTE = "attribute"
    
    

class Gender(ENUMType):
    MALE = 'male'
    FEMALE = 'female'
    
    
class Age(ENUMType):
    _13 = '13'
    _18 = '18'
    _25 = '25'
    _35 = '35'
    _45 = '45'
    _55 = '55'
    _65 = '65'

class AgeRange():
    
    ranges = {
        13: (13, 17),
        18: (18, 24),
        25: (25, 34),
        35: (35, 44),
        45: (45, 54),
        55: (55, 64),
        65: (65, 120)
    }
    
    @staticmethod
    def get_range(age):
        for value_pair in AgeRange.ranges.values():
            if int(age) in range(value_pair[0], value_pair[1]+1 if value_pair[1] != 65 else 120):
                return value_pair
            
            
    @staticmethod
    def get_range_str(age):
        for value_pair in AgeRange.ranges.values():
            if int(age) in range(value_pair[0], value_pair[1]+1 if value_pair[1] != 65 else 120):
                return str(value_pair[0])+"-"+("" if value_pair[0] == 65 else str(value_pair[1]))
        return None
    
            
    @staticmethod
    def get_range_dict():
        results = {}
        for value_pair in AgeRange.ranges.values():
            for age in range(value_pair[0], value_pair[1]+1):
                results[age] = str(value_pair[0])+"-"+("" if value_pair[0] == 65 else str(value_pair[1]))
        return results
    
    @staticmethod
    def get_random_age(age):
        age_pair = AgeRange.get_range(int(age))
        import random
        return random.randint(age_pair[0], age_pair[1] if age_pair[1] != 120 else 75 )
    
    @staticmethod
    def get_min(age):
        return AgeRange.get_range(int(age))[0]
    
    @staticmethod
    def get_max(age):
        return AgeRange.get_range(int(age))[1]

    
class Ethnicity(ENUMType):
    ARAB = 'arab'
    ASIAN = 'asian'
    WHITE = 'white'
    BLACK = 'black'
    PACIFIC_ISLANDER = 'pacific islander'
    NATIVE_AMERICAN = 'native american'
    HISPANIC = 'hispanic'
        
    
class DataSourceType(ENUMType):
    YOUTUBE = "youtube"
    FACEBOOK = "facebook"
    TWITTER = "twitter"
    INSTAGRAM = "instagram"
    FACEBOOKADS = "facebookads"
    GOOGLEANALYTICS = "googleanalytics"
    INHOUSE = "inhouse"


    @staticmethod
    def get_label(data_source_type):
        if data_source_type == DataSourceType.YOUTUBE.value:
            return "YouTube"
        elif data_source_type == DataSourceType.FACEBOOK.value:
            return "Facebook"
        elif data_source_type == DataSourceType.TWITTER.value:
            return "Twitter"
        elif data_source_type == DataSourceType.INSTAGRAM.value:
            return "Instagram"
        elif data_source_type == DataSourceType.FACEBOOKADS.value:
            return "FacebookAds"
        elif data_source_type == DataSourceType.GOOGLEANALYTICS.value:
            return "Google Analytics"
        elif data_source_type == DataSourceType.INHOUSE.value:
            return "In house data"
    
    @staticmethod
    def get_content_type(data_source_type):
        if data_source_type == DataSourceType.YOUTUBE.value:
            return "video"
        elif data_source_type == DataSourceType.FACEBOOK.value:
            return "post"
        elif data_source_type == DataSourceType.TWITTER.value:
            return "tweet"
        elif data_source_type == DataSourceType.INSTAGRAM.value:
            return "post"
        elif data_source_type == DataSourceType.FACEBOOKADS.value:
            return "ads"
        elif data_source_type == DataSourceType.GOOGLEANALYTICS.value:
            return "page"
        elif data_source_type == DataSourceType.INHOUSE.value:
            return "instance"
    
    @staticmethod
    def get_engagement_type(data_source_type):
        if data_source_type == DataSourceType.YOUTUBE.value:
            return "views"
        elif data_source_type == DataSourceType.FACEBOOKADS.value:
            return "clicks"
        elif data_source_type == DataSourceType.GOOGLEANALYTICS.value:
            return "unique page views"
        elif data_source_type == DataSourceType.FACEBOOK.value:
            return "seconds"
        else:
            return "instances"
    
    @staticmethod
    def get_types_and_labels():
        return [
            (DataSourceType.YOUTUBE.value, DataSourceType.get_label(DataSourceType.YOUTUBE.value) ),
            (DataSourceType.GOOGLEANALYTICS.value, DataSourceType.get_label(DataSourceType.GOOGLEANALYTICS.value) ),
            (DataSourceType.FACEBOOK.value, DataSourceType.get_label(DataSourceType.FACEBOOK.value) ),
            (DataSourceType.FACEBOOKADS.value, DataSourceType.get_label(DataSourceType.FACEBOOKADS.value) ),
            (DataSourceType.TWITTER.value, DataSourceType.get_label(DataSourceType.TWITTER.value) ),
            (DataSourceType.INSTAGRAM.value, DataSourceType.get_label(DataSourceType.INSTAGRAM.value) ),
            (DataSourceType.INHOUSE.value, DataSourceType.get_label(DataSourceType.INHOUSE.value) )
            ]
    
