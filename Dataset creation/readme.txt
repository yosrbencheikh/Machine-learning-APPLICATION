
* original_data=json([[country_code, value0, value1, value2], [country_code, value0, value1, value2]])

* view_time_data=json({country_code:value1})

	- value0 is the count of views in the country
	- value1 is the average view duration in the country
	- value2 is a list of ['age18-25', 'M', view_count]

To run the code :

	python3 main_set.py --topics_file_path=data/vocabulary.csv --train_data_pattern=/home/apexiahr/VoraInsight_04_12_2019/Persona_project_workspace/workspace/data/yt8m/youtube-video-validation/validate*.tfrecord
