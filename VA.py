# Import PyMongo to Connect the MongoClient and Find the ServiceAreaName, SourceName , and BusinessYear 
# across the country each state
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
client = MongoClient('localhost',27017)
# print('Connect to the MongoDB Client')
db = client.health_insurance
collection = db.health_insurance_service
data = pd.DataFrame(list(collection.find()))
state_count_df = data.groupby('StateCode').count()
a = state_count_df[['ServiceAreaName','SourceName','BusinessYear']]
a.plot.bar(figsize=(10,10))
plt.show() 

source_count_df = data.groupby('SourceName').count()
b = state_count_df[['SourceName','County']]
b.plot.bar(figsize=(10,10))
plt.show()