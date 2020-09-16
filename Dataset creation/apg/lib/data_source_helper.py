from googleapiclient.discovery import build
from apg.lib.common_helper import CommonHelper
from config import config
from apg.lib.enums import ContentAttribute
from apg.lib.youtube_data_source import YouTubeDataSource
from datetime import datetime
import os, pandas as pd, time

GOOGLE_CLIENT_API_NAME ='youtube'
GOOGLE_CLIENT_API_VERSION ='v3'

'''
json_original_data={}
'''

class DataSourceHelper() :
    helper = None

    def __init__(self) :
        self.service = build(GOOGLE_CLIENT_API_NAME, GOOGLE_CLIENT_API_VERSION, developerKey=config.DEVELOPER_KEY)

    @staticmethod
    def read_video_list(video_ids, helper=None) :
        if helper is None :
            helper = DataSourceHelper.get_instance()
        try:
            request = (helper.service).videos().list(part="snippet,contentDetails, statistics", id=','.join(video_ids))
            response = request.execute()
        except Exception as e :
            print ("Enable to retrieve video IDs due to %r"%str(e))
            if 'Daily Limit Exceeded' in str(e) :
                print("Go to sleep for 360 seconds...")
                time.sleep(360)
                return DataSourceHelper.read_video_list(video_ids, helper)
            return None, None
        json_resp = CommonHelper.to_json(response)
        videos  = DataSourceHelper.collect_videos(json_resp['items']) if 'items' in json_resp and len(json_resp['items']) > 0 else None
        return videos

    @staticmethod
    def read_video(video_id, helper=None) :
        if helper is None :
            helper = DataSourceHelper.get_instance()
        try:
            request = (helper.service).videos().list(part="snippet,contentDetails, statistics", id=video_id)
            response = request.execute()
        except Exception as e :
            print ("Enable to retrieve video ID=%r due to %r"%(video_id,str(e)))
            if 'Daily Limit Exceeded' in str(e) :
                print("Go to sleep for 360 seconds...")
                time.sleep(360)
                return DataSourceHelper.read_video(video_id, helper)
            return None, None
        json_resp = CommonHelper.to_json(response)
        channel_info, video_info  = DataSourceHelper.collect_video_info(json_resp['items'][0]) if 'items' in json_resp and len(json_resp['items']) > 0 else (None, None)
        return channel_info, video_info

    @staticmethod
    def save_datasource(channel, type_data) :
        if channel.content_dataframe is not None and channel.topic_configuration is not None :

            file_name = config.DATA_SOURCE_CHANNEL_FOLDER+type_data +'/'+ type_data
            
            with open(file_name+'_contents.csv', 'w') as content_file:
                channel.content_dataframe.to_csv(path_or_buf=content_file)
            if channel.content_comments is not None :
                with open(file_name+'_comments.csv', 'w') as comment_file :
                    channel.content_comments.to_csv(path_or_buf=comment_file)
            
            with open(file_name+'_topics.csv', 'w') as topics_file :
                channel.topic_configuration.to_csv(path_or_buf=topics_file)

        print('end saving')                


    
    @staticmethod
    def load_data_source(data_type) :
        path = config.DATA_SOURCE_CHANNEL_FOLDER+data_type +'/'
        data_source = YouTubeDataSource()
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.is_file() and os.path.splitext(entry.name)[1] == '.csv':
                    if data_type in entry.name :
                        if 'contents' in entry.name :
                            df_contents = pd.read_csv(path+entry.name, index_col=0)
                            if df_contents is not None and not df_contents.empty:
                                    data_source.set_content_dataframe(df_contents)

                        elif 'comments' in entry.name : 
                            df_comments = pd.read_csv(path+entry.name, index_col=0)
                            if df_comments is not None and not df_comments.empty:
                                    data_source.set_content_comments(df_comments)

                        elif 'topics' in entry.name : 
                            df_topics = pd.read_csv(path+entry.name, index_col=0)
                            if df_topics is not None and not df_topics.empty:
                                    data_source.set_topic_configuration(df_topics)
        return data_source

    @staticmethod
    def get_instance() :
        if DataSourceHelper.helper is None :
            DataSourceHelper.helper = DataSourceHelper()
        return DataSourceHelper.helper


    @staticmethod
    def collect_videos(json_content) :
        videos = []
        print('start reading videos json')
        for video_json in json_content :
            video_info = {}
            video_info[ContentAttribute.ID.value] = video_json["id"]
            if "statistics" in video_json and video_json["statistics"] :
                video_info[ContentAttribute.CONTENT_VIEW_COUNT.value] = int(video_json["statistics"]["viewCount"]) if "viewCount" in video_json["statistics"] else 0
                video_info[ContentAttribute.CONTENT_LIKE_COUNT.value] = int(video_json["statistics"]["likeCount"]) if "likeCount" in video_json["statistics"] else 0
                video_info[ContentAttribute.CONTENT_DISLIKE_COUNT.value] = int(video_json["statistics"]["dislikeCount"]) if "dislikeCount" in video_json["statistics"] else 0
                video_info[ContentAttribute.CONTENT_COMMENT_COUNT.value] = int(video_json["statistics"]["commentCount"]) if "commentCount" in video_json["statistics"] else 0 
            if (ContentAttribute.CONTENT_VIEW_COUNT.value not in video_info or video_info[ContentAttribute.CONTENT_VIEW_COUNT.value] < 5000) :
                continue

            if "snippet" in video_json and video_json["snippet"] :
                if "title" in video_json["snippet"] :
                    video_info[ContentAttribute.TITLE.value] = video_json["snippet"]["title"]
                if "description" in video_json["snippet"] :
                    video_info[ContentAttribute.DECRIPTION.value] = video_json["snippet"]["description"]
                if 'channelId' in video_json["snippet"] :
                    video_info[ContentAttribute.CHANNEL_ID.value] = video_json["snippet"]['channelId']
                    video_info[ContentAttribute.CHANNEL_NAME.value] = video_json["snippet"]['channelTitle']
                if "thumbnails" in video_json["snippet"] and video_json["snippet"]["thumbnails"] and "default" in video_json["snippet"]["thumbnails"] and video_json["snippet"]["thumbnails"]["default"] and 'url' in video_json["snippet"]["thumbnails"]["default"]["url"]:
                    video_info[ContentAttribute.THUMBNAIL_URL.value] = video_json["snippet"]["thumbnails"]["default"]["url"]
                video_info[ContentAttribute.URL.value] = "https://www.youtube.com/watch?v=" + video_json["id"]
                if "publishedAt" in video_json["snippet"] and video_json["snippet"]["publishedAt"] :
                    video_info[ContentAttribute.PUBLISHED_IN.value] = datetime.strptime(video_json["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
                if 'tags' in video_json["snippet"]:
                    video_info[ContentAttribute.TAGS.value] = video_json["snippet"]['tags']
                if 'defaultLanguage' in video_json["snippet"] :
                    video_info[ContentAttribute.LANGUAGE.value] = video_json["snippet"]["defaultLanguage"]

            if "contentDetails" in video_json and video_json["contentDetails"] :
                if 'duration' in video_json["contentDetails"]:
                    video_info[ContentAttribute.CONTENT_AVERAGE_TIME.value] = video_json["contentDetails"]['duration']

            if (ContentAttribute.CONTENT_COMMENT_COUNT.value in video_info and video_info[ContentAttribute.CONTENT_COMMENT_COUNT.value] >= 5) :
                video_info[ContentAttribute.COMMENTS.value] = DataSourceHelper.collect_comment(video_info[ContentAttribute.ID.value], DataSourceHelper.get_instance)
            else : 
                video_info[ContentAttribute.COMMENTS.value] = []
            videos.append(video_info)
        print(len(videos))
        return videos


    @staticmethod
    def collect_video_info(json_content) :
        video_info = {}
        video_info[ContentAttribute.ID.value] = json_content["id"]
        if "statistics" in json_content and json_content["statistics"] :
            video_info[ContentAttribute.CONTENT_VIEW_COUNT.value] = int(json_content["statistics"]["viewCount"]) if "viewCount" in json_content["statistics"] else 0
            video_info[ContentAttribute.CONTENT_LIKE_COUNT.value] = int(json_content["statistics"]["likeCount"]) if "likeCount" in json_content["statistics"] else 0
            video_info[ContentAttribute.CONTENT_DISLIKE_COUNT.value] = int(json_content["statistics"]["dislikeCount"]) if "dislikeCount" in json_content["statistics"] else 0
            video_info[ContentAttribute.CONTENT_COMMENT_COUNT.value] = int(json_content["statistics"]["commentCount"]) if "commentCount" in json_content["statistics"] else 0 
        if (ContentAttribute.CONTENT_VIEW_COUNT.value in video_info and video_info[ContentAttribute.CONTENT_VIEW_COUNT.value] < 5000) :
            return None, None
        channel_id = None
        channel_name = None
        if "snippet" in json_content and json_content["snippet"] :
            if "title" in json_content["snippet"] :
                video_info[ContentAttribute.TITLE.value] = json_content["snippet"]["title"]
            if "description" in json_content["snippet"] :
                video_info[ContentAttribute.DECRIPTION.value] = json_content["snippet"]["description"]
            if 'channelId' in json_content["snippet"] :
                channel_id = json_content["snippet"]['channelId']
                channel_name = json_content["snippet"]['channelTitle']
            if "thumbnails" in json_content["snippet"] and json_content["snippet"]["thumbnails"] and "default" in json_content["snippet"]["thumbnails"] and json_content["snippet"]["thumbnails"]["default"] and 'url' in json_content["snippet"]["thumbnails"]["default"]["url"]:
                video_info[ContentAttribute.THUMBNAIL_URL.value] = json_content["snippet"]["thumbnails"]["default"]["url"]
            video_info[ContentAttribute.URL.value] = "https://www.youtube.com/watch?v=" + json_content["id"]
            if "publishedAt" in json_content["snippet"] and json_content["snippet"]["publishedAt"] :
                video_info[ContentAttribute.PUBLISHED_IN.value] = datetime.strptime(json_content["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if 'tags' in json_content["snippet"]:
                video_info[ContentAttribute.TAGS.value] = json_content["snippet"]['tags']
            if 'defaultLanguage' in json_content["snippet"] :
                video_info[ContentAttribute.LANGUAGE.value] = json_content["snippet"]["defaultLanguage"]

        if "contentDetails" in json_content and json_content["contentDetails"] :
            if 'duration' in json_content["contentDetails"]:
                video_info[ContentAttribute.CONTENT_AVERAGE_TIME.value] = json_content["contentDetails"]['duration']

        video_info[ContentAttribute.COMMENTS.value] = DataSourceHelper.collect_comment(video_info[ContentAttribute.ID.value], DataSourceHelper.get_instance)

        return {ContentAttribute.CHANNEL_ID.value:channel_id, ContentAttribute.CHANNEL_NAME.value:channel_name}, video_info


    @staticmethod
    def collect_comment(video_id, helper=None):
        if helper :
            helper = DataSourceHelper.get_instance()
        try:
            request = (helper.service).commentThreads().list(part="snippet", videoId=video_id)
            response = request.execute()
        except Exception as e :
            print ("Enable to retrieve comments for video ID=%r due to %r"%(video_id,str(e)))
            if 'Daily Limit Exceeded' in str(e) :
                print('Go to sleep for 360 seconds...')
                time.sleep(360)
                return DataSourceHelper.collect_comment(video_id, helper)

            return []
        json_resp = CommonHelper.to_json(response)
        
        json_comments = json_resp['items'] if 'items' in json_resp else None

        collected_comments = []
        if json_comments :
            for json_comment in json_comments :
                if 'id' in json_comment and "snippet" in json_comment:
                    comment = {}
                    comment[ContentAttribute.COMMENT_ID.value] = json_comment['id']
                    comment[ContentAttribute.COMMENT_MESSAGE.value] = json_comment["snippet"]["topLevelComment"]["snippet"]['textDisplay'].replace('\x00', '')
                    comment[ContentAttribute.COMMENT_AUTHOR.value] = json_comment["snippet"]["topLevelComment"]["snippet"]['authorDisplayName'].replace('\x00', '')
                    comment[ContentAttribute.COMMENT_LIKE_COUNT.value] = int(json_comment["snippet"]["topLevelComment"]["snippet"]['likeCount'])
                    comment[ContentAttribute.COMMENT_CREATION_DT.value] = datetime.strptime(json_comment["snippet"]["topLevelComment"]["snippet"]['updatedAt'].replace(".000Z", ""), "%Y-%m-%dT%H:%M:%S")
                    comment[ContentAttribute.COMMENT_REPLY_COUNT.value] = int(json_comment["snippet"]["totalReplyCount"])
                    collected_comments.append(comment)
        return collected_comments
