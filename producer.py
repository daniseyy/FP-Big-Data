from time import sleep
from json import dumps
from kafka import KafkaProducer
import os
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
    
dataset_folder_path = os.path.join(os.path.dirname(os.getcwd()), 'FP-Big-Data\\Dataset')
dataset_file_path = os.path.join(dataset_folder_path, 'business_ratings.csv')
batch = 3
batch_max = 10000
batch_flag = 0
with open(dataset_file_path,"r", encoding="utf-8") as f:
    for row in f:
    	if batch_flag > batch*batch_max:
    	   break
    	producer.send('yelpdata', value=row)
    	batch_flag += 1
    	print(row)
    	sleep(0.00001)