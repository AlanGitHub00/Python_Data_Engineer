import VA
# The following plot shows the sourcename for the country.
source_count_df = data.groupby('SourceName').count()
b = state_count_df[['SourceName','County']]
b.plot.bar(figsize=(10,10))
plt.show()
