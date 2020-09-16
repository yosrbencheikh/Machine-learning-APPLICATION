from apg.lib.enums import DataSourceType, AgeRange, Gender
import pycountry, pandas as pd, random, isodate
from apg.lib.enums import ContentAttribute
from apg.lib.common_helper import CommonHelper

CONTENT_COLUMNS =['data_source_id', 
                    'data_source_name',
                    'id', 'title', 
                    'description', 
                    'language', 
                    'thumbnail_url', 
                    'url', 'created_dt',
                    'tags', 'original_data', 
                    'view_time_data'
                ]
COMMENT_COLUMNS = ['data_source_id', 'data_source_name', 
                    'content_id', 'comment_id', 'message', 
                    'author', 'number_of_likes', 'number_of_replies', 'created_dt'
                ]
TOPIC_COMFIGURATION_COLUMNS = ['data_source_id', 'data_source_name', 
                                'content_id', 'title', 'description', 
                                'topics', 'categories'
                            ]

class YouTubeDataSource():

    def __init__(self):
#    def __init__(self, channel_id, channel_name=None):
#        self.channel_id = channel_id
#        self.channel_name = channel_name
        self.content_dataframe = None
        self.content_comments = None 
        self.topic_configuration = None 

    def add_videos(self, video_list, videos_labels, topics) :
        for video in video_list :
            target_topics = topics[topics['Index'].isin(videos_labels[video[ContentAttribute.ID.value]])]
            self.add_video_youtube(video, {ContentAttribute.CHANNEL_ID.value:video[ContentAttribute.CHANNEL_ID.value], ContentAttribute.CHANNEL_NAME.value:video[ContentAttribute.CHANNEL_NAME.value]}, target_topics)

    def add_video_youtube(self, video_info, channel_info, topics) :
        if self.content_dataframe is None :
            self.content_dataframe = pd.DataFrame(columns=CONTENT_COLUMNS)
        if self.topic_configuration is None :
            self.topic_configuration = pd.DataFrame(columns=TOPIC_COMFIGURATION_COLUMNS)
        print('Adding a video to the data source')

        original_data, view_time_data = YouTubeDataSource.view_count_time_demographic(video_info[ContentAttribute.CONTENT_VIEW_COUNT.value], video_info[ContentAttribute.CONTENT_AVERAGE_TIME.value])
        new_row_content = {CONTENT_COLUMNS[0]:channel_info[ContentAttribute.CHANNEL_ID.value], CONTENT_COLUMNS[1]:channel_info[ContentAttribute.CHANNEL_NAME.value], 
                            CONTENT_COLUMNS[2]:video_info[ContentAttribute.ID.value], 
                            CONTENT_COLUMNS[3]:video_info[ContentAttribute.TITLE.value], 
                            CONTENT_COLUMNS[4]:video_info[ContentAttribute.DECRIPTION.value],
                            CONTENT_COLUMNS[5]:video_info[ContentAttribute.LANGUAGE.value] if ContentAttribute.LANGUAGE.value in video_info else None,
                            CONTENT_COLUMNS[6]:video_info[ContentAttribute.THUMBNAIL_URL.value] if ContentAttribute.THUMBNAIL_URL.value in video_info else None,
                            CONTENT_COLUMNS[7]:video_info[ContentAttribute.URL.value] if ContentAttribute.URL.value in video_info else None,
                            CONTENT_COLUMNS[8]:video_info[ContentAttribute.PUBLISHED_IN.value] if ContentAttribute.PUBLISHED_IN.value in video_info else None,
                            CONTENT_COLUMNS[9]:video_info[ContentAttribute.TAGS.value] if ContentAttribute.TAGS.value in video_info else None, 
                            CONTENT_COLUMNS[10]:CommonHelper.to_json(original_data), 
                            CONTENT_COLUMNS[11]:CommonHelper.to_json(view_time_data)
                        }
        self.content_dataframe = self.content_dataframe.append(new_row_content, ignore_index=True)

        if len(video_info[ContentAttribute.COMMENTS.value]) > 0 :
            if self.content_comments is None :
                self.content_comments = pd.DataFrame(columns=COMMENT_COLUMNS)

        for comment in video_info[ContentAttribute.COMMENTS.value] :

            new_row_comment = {COMMENT_COLUMNS[0]:channel_info[ContentAttribute.CHANNEL_ID.value], 
                                COMMENT_COLUMNS[1]:channel_info[ContentAttribute.CHANNEL_NAME.value], 
                                COMMENT_COLUMNS[2]:video_info[ContentAttribute.ID.value], 
                                COMMENT_COLUMNS[3]:comment[ContentAttribute.COMMENT_ID.value],
                                COMMENT_COLUMNS[4]:comment[ContentAttribute.COMMENT_MESSAGE.value], 
                                COMMENT_COLUMNS[5]:comment[ContentAttribute.COMMENT_AUTHOR.value] if ContentAttribute.COMMENT_AUTHOR.value in video_info else None, 
                                COMMENT_COLUMNS[6]:comment[ContentAttribute.COMMENT_LIKE_COUNT.value] if ContentAttribute.COMMENT_LIKE_COUNT.value in comment else 0,
                                COMMENT_COLUMNS[7]:comment[ContentAttribute.COMMENT_REPLY_COUNT.value] if ContentAttribute.COMMENT_REPLY_COUNT.value in comment else 0,
                                COMMENT_COLUMNS[8]:comment[ContentAttribute.COMMENT_CREATION_DT.value] if ContentAttribute.COMMENT_CREATION_DT.value in comment else None
                            }
            self.content_comments = self.content_comments.append(new_row_comment, ignore_index=True)
        categories = [topic for topic in topics.Vertical1.unique() if not pd.isna(topic)]
        categories = categories + [topic for topic in topics.Vertical2.unique() if not pd.isna(topic) and topic not in categories]
        categories = categories + [topic for topic in topics.Vertical3.unique() if not pd.isna(topic) and topic not in categories]

        new_row_topics = {TOPIC_COMFIGURATION_COLUMNS[0]:channel_info[ContentAttribute.CHANNEL_ID.value],
                            TOPIC_COMFIGURATION_COLUMNS[1]:channel_info[ContentAttribute.CHANNEL_NAME.value],
                            TOPIC_COMFIGURATION_COLUMNS[2]:video_info[ContentAttribute.ID.value],
                            TOPIC_COMFIGURATION_COLUMNS[3]:video_info[ContentAttribute.TITLE.value],
                            TOPIC_COMFIGURATION_COLUMNS[4]:video_info[ContentAttribute.DECRIPTION.value],
                            TOPIC_COMFIGURATION_COLUMNS[5]:','.join([topic for topic in topics.Name.unique() if not pd.isna(topic)]),
                            TOPIC_COMFIGURATION_COLUMNS[6]:','.join(categories)
                        }
        self.topic_configuration = self.topic_configuration.append(new_row_topics, ignore_index=True)

    def add_video(self, video_info, topics):
        if self.content_dataframe is None :
            self.content_dataframe = pd.DataFrame(columns=CONTENT_COLUMNS)
        if self.topic_configuration is None :
            self.topic_configuration = pd.DataFrame(columns=TOPIC_COMFIGURATION_COLUMNS)

        original_data, view_time_data = YouTubeDataSource.view_count_time_demographic(video_info[ContentAttribute.CONTENT_VIEW_COUNT.value], video_info[ContentAttribute.CONTENT_AVERAGE_TIME.value])
        new_row_content = {CONTENT_COLUMNS[0]:self.channel_id, CONTENT_COLUMNS[1]:self.channel_name, 
                            CONTENT_COLUMNS[2]:video_info[ContentAttribute.ID.value], 
                            CONTENT_COLUMNS[3]:video_info[ContentAttribute.TITLE.value], 
                            CONTENT_COLUMNS[4]:video_info[ContentAttribute.DECRIPTION.value],
                            CONTENT_COLUMNS[5]:video_info[ContentAttribute.LANGUAGE.value] if ContentAttribute.LANGUAGE.value in video_info else None,
                            CONTENT_COLUMNS[6]:video_info[ContentAttribute.THUMBNAIL_URL.value] if ContentAttribute.THUMBNAIL_URL.value in video_info else None,
                            CONTENT_COLUMNS[7]:video_info[ContentAttribute.URL.value] if ContentAttribute.URL.value in video_info else None,
                            CONTENT_COLUMNS[8]:video_info[ContentAttribute.PUBLISHED_IN.value] if ContentAttribute.PUBLISHED_IN.value in video_info else None,
                            CONTENT_COLUMNS[9]:video_info[ContentAttribute.TAGS.value] if ContentAttribute.TAGS.value in video_info else None, 
                            CONTENT_COLUMNS[10]:CommonHelper.to_json(original_data), 
                            CONTENT_COLUMNS[11]:CommonHelper.to_json(view_time_data)
                        }
        self.content_dataframe = self.content_dataframe.append(new_row_content, ignore_index=True)
        if len(video_info[ContentAttribute.COMMENTS.value]) > 0 :
            if self.content_comments is None :
                self.content_comments = pd.DataFrame(columns=COMMENT_COLUMNS)

        for comment in video_info[ContentAttribute.COMMENTS.value] :

            new_row_comment = {COMMENT_COLUMNS[0]:self.channel_id, 
                                COMMENT_COLUMNS[1]:self.channel_name, 
                                COMMENT_COLUMNS[2]:video_info[ContentAttribute.ID.value], 
                                COMMENT_COLUMNS[3]:comment[ContentAttribute.COMMENT_ID.value],
                                COMMENT_COLUMNS[4]:comment[ContentAttribute.COMMENT_MESSAGE.value], 
                                COMMENT_COLUMNS[5]:comment[ContentAttribute.COMMENT_AUTHOR.value] if ContentAttribute.COMMENT_AUTHOR.value in video_info else None, 
                                COMMENT_COLUMNS[6]:comment[ContentAttribute.COMMENT_LIKE_COUNT.value] if ContentAttribute.COMMENT_LIKE_COUNT.value in comment else 0,
                                COMMENT_COLUMNS[7]:comment[ContentAttribute.COMMENT_REPLY_COUNT.value] if ContentAttribute.COMMENT_REPLY_COUNT.value in comment else 0,
                                COMMENT_COLUMNS[8]:comment[ContentAttribute.COMMENT_CREATION_DT.value] if ContentAttribute.COMMENT_CREATION_DT.value in comment else None
                            }
            self.content_comments = self.content_comments.append(new_row_comment, ignore_index=True)
        categories = [topic for topic in topics.Vertical1.unique() if not pd.isna(topic)]
        categories = categories + [topic for topic in topics.Vertical2.unique() if not pd.isna(topic) and topic not in categories]
        categories = categories + [topic for topic in topics.Vertical3.unique() if not pd.isna(topic) and topic not in categories]

        new_row_topics = {TOPIC_COMFIGURATION_COLUMNS[0]:self.channel_id,
                            TOPIC_COMFIGURATION_COLUMNS[1]:self.channel_name,
                            TOPIC_COMFIGURATION_COLUMNS[2]:video_info[ContentAttribute.ID.value],
                            TOPIC_COMFIGURATION_COLUMNS[3]:video_info[ContentAttribute.TITLE.value],
                            TOPIC_COMFIGURATION_COLUMNS[4]:video_info[ContentAttribute.DECRIPTION.value],
                            TOPIC_COMFIGURATION_COLUMNS[5]:','.join([topic for topic in topics.Name.unique() if not pd.isna(topic)]),
                            TOPIC_COMFIGURATION_COLUMNS[6]:','.join(categories)
                        }
        self.topic_configuration = self.topic_configuration.append(new_row_topics, ignore_index=True)

    @staticmethod
    def random_age_gender(demographic) :
        ls_age_range = list(AgeRange.ranges.items())
        gender_list = [Gender.MALE.value, Gender.FEMALE.value]
        while True :
            age = random.choice(ls_age_range)
            gender = random.choice(gender_list)
            if ('age'+str(age[0])+'-'+str(age[1]), gender) not in demographic :
                return ('age'+str(age[1][0])+'-'+str(age[1][1]), gender)



    @staticmethod
    def view_count_time_demographic(content_view_count, view_time):
        used_demographic = {}
        rest_count = content_view_count
        original_data = []
        duration = isodate.parse_duration(view_time)
        view_time = int(duration.total_seconds()) 
        view_time_data = {}
        while rest_count > 0 :
            view_count_by_country = []
            country_code = random.choice([x for x in CommonHelper.get_country_codes() if x not in list(used_demographic.keys())])
            random_count = random.choice(range(10,rest_count+1))
            if rest_count - random_count < 20 :
                random_count = rest_count
                rest_count = 0
            else :
                rest_count = rest_count - random_count
            view_count_by_country.append(country_code)
            view_count_by_country.append(random_count)
            used_demographic[country_code] = []
            count_demographic = []
            while random_count > 0 :
                age_range, gender = YouTubeDataSource.random_age_gender(used_demographic[country_code] if country_code in used_demographic else [])
                if len(used_demographic[country_code]) == len(AgeRange.ranges.items())*2 - 1 or random_count < 20:
                    random_view_demographic = random_count
                    random_count = 0
                else :
                    random_view_demographic = random.choice(range(10,random_count+1))
                    random_count = random_count - random_view_demographic
                used_demographic[country_code].append((age_range, gender))
                count_demographic.append([age_range, gender, random_view_demographic])
            duration_by_country = random.choice(range(120 if view_time > 120 else view_time,view_time+1))
            view_count_by_country.append(duration_by_country)
            view_time_data[country_code] = duration_by_country
            view_count_by_country.append(count_demographic)
            original_data.append(view_count_by_country)
        return original_data, view_time_data
                



    def set_content_dataframe(self, content_df) :
        self.content_dataframe = content_df

    def set_content_comments(self, comments_df) :
        self.content_comments = comments_df

    def set_topic_configuration(self, topics_df) :
        self.topic_configuration = topics_df