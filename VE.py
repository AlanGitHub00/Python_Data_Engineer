# Import pymongo module and using insurance dataset
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
client = MongoClient('localhost',27017)
# print('Connect to the MongoDB Client')
e1 = db.health_insurance_insurancecharges
e2 = pd.DataFrame(list(e1.find()))

e3=('The number of mother who smoke and also have children are ', e2[(e2['sex']=='female') & \
                                                                 (e2['smoker']=='yes') & \
                                                                 (e2['children']>0)]['_id'].count())
print(e3)
# The number of monther who smoke and also have children are 62 according to the dataset.
