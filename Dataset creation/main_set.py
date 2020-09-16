import tensorflow as tf
import pandas as pd
from tensorflow import app
from tensorflow import flags
from tensorflow import gfile
from apg.lib.common_helper import CommonHelper
from apg.lib.data_source_helper import DataSourceHelper
from apg.lib.youtube_data_source import YouTubeDataSource
from apg.lib.enums import ContentAttribute
from config import config

FLAGS = flags.FLAGS
STEP_LOAD = 5000
data_source_youtube = YouTubeDataSource()

if __name__ == "__main__":
  # Dataset flags.
  flags.DEFINE_string(
      "train_data_pattern", "",
      "File glob for the training dataset. If the files refer to Frame Level "
      "features (i.e. tensorflow.SequenceExample), then set --reader_type "
      "format. The (Sequence)Examples are expected to have 'rgb' byte array "
      "sequence feature as well as a 'labels' int64 context feature.")

  flags.DEFINE_string("topics_file_path", "", "File containing a set of topics")

  # Training flags.
  flags.DEFINE_integer("batch_size", 1024,
                       "How many examples to process per batch for training.")

  # Other flags.
  flags.DEFINE_integer("num_readers", 8,
                       "How many threads to use for reading input files.")

def decode_record(data_record):

    feature_map = {
        "id": tf.FixedLenFeature([], tf.string),
        "labels": tf.VarLenFeature(tf.int64)
    }
    sample = tf.parse_single_example(data_record, feature_map)
    video_id = sample["id"]
    labels = sample["labels"].values
    return video_id,labels

def load_topics(topics_file_path) :
    topics_dataframe = pd.read_csv(topics_file_path)
    return topics_dataframe

def processing_videos(videos, list_video_labels, topics, data_pattern) :
    global data_source_youtube
    list_videos = DataSourceHelper.read_video_list(videos, DataSourceHelper.get_instance())
    if list_videos :
        try:
            data_source_youtube.add_videos(list_videos, list_video_labels, topics)
            del list_videos

        except Exception as e :
            print('Enable to add videos to dataframe: %r'%e)

def youtube_dataframes_from_data_tensors(data_pattern, topics, start_index=0, end_index=1000000) :
    files = gfile.Glob(data_pattern)
    num_videos_read = 0
    still_data_to_read = False
    list_videos_labels = {}
    if not files:
      raise IOError("Unable to find training files. data_pattern='" +
                    data_pattern + "'.")
    print("Number of training files: %s."%str(len(files)))
    dataset = tf.data.TFRecordDataset(filenames=files)
    dataset = dataset.map(decode_record)
    iterator = dataset.make_one_shot_iterator()
    next_element = iterator.get_next()

    with tf.Session() as sess :
        try:
            print(start_index)
            while True:
                id_video, video_labels = sess.run(next_element)
                id_video = id_video.decode("utf-8")
                if num_videos_read >= start_index :
                    try :
                        yt_video_id = CommonHelper.lookup_id(id_video)
                        if yt_video_id :
                                list_videos_labels[yt_video_id] = video_labels                     
                                print('The video ID=%r has labels %r'%(yt_video_id,video_labels))
                    except ConnectionError as e :
                        print('Enable to read data: %r'%e)
                num_videos_read+=1
                if num_videos_read >= end_index:
                    still_data_to_read = True
                    break
        except Exception as e:
            print('Enable to read data: %r'%e)
    del dataset, iterator
    print(len(list(list_videos_labels.keys())))
    video_ids_chunk = list(CommonHelper.chunks(list(list_videos_labels.keys()), 50))
    if video_ids_chunk is not None :
        for index, target_content_ids in enumerate(video_ids_chunk):
            try :
                processing_videos(target_content_ids, list_videos_labels, topics, data_pattern)
            except Exception as e:
                print('Enable to process a set of video: %r'%e)

    del list_videos_labels, video_ids_chunk

    DataSourceHelper.save_datasource(data_source_youtube, data_pattern.split('/')[::-1][0].split('*')[0])
    print("The total of videos that have been read is %r"%(num_videos_read))

    if not still_data_to_read :
        raise Exception

def main(unused_argv) :
    global data_source_youtube
    print(FLAGS.topics_file_path)
    data_source_youtube = DataSourceHelper.load_data_source(FLAGS.train_data_pattern.split('/')[::-1][0].split('*')[0])
    topics = load_topics(topics_file_path=FLAGS.topics_file_path)
    for index in range(0, 50000, STEP_LOAD) :
        youtube_dataframes_from_data_tensors(data_pattern=FLAGS.train_data_pattern, topics=topics, start_index=index, end_index=index+STEP_LOAD)
        
if __name__ == '__main__':
    app.run()