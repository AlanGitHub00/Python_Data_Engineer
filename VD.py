# Import PyMongo to Connect the MongoClient and using Benefit Cost Sharing dataset.
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
client = MongoClient('localhost',27017)
# print('Connect to the MongoDB Client')
d1 = db.health_insurance_benefit
d2 = pd.DataFrame(list(d1.find()))
d3 = d2[['BenefitName','StateCode']].groupby(['StateCode'])['BenefitName'].count()
d3.plot.bar(figsize = (10,10))
plt.show()
# The plot will show the benefit plans in each state, Wisconsin has the highest benefit plans in the country.
