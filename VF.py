# Import PyMongo to Connect the MongoClient and using insurance dataset to find out the region has the highest rate of smokers.
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
client = MongoClient('localhost',27017)
print('Connect to the MongoDB Client')
f1 = db.health_insurance_insurancecharges
f2 = pd.DataFrame(list(f1.find()))
f3 = f2.groupby('region').count()['_id']
sm = f2[f2.smoker == 'yes'].groupby('region').count()['_id']
xyz = sm / f3
xyz.plot.bar(figsize=(10,10))
plt.show()
# The following graph shows that Southeastern region has the hightest rate more smokers.
