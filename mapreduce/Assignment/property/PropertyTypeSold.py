from mrjob.job import MRJob

#mapreduce

class PropertyType(MRJob):
    
    def mapper(self, key, line):
       ID,Year,Month,Type,Price = line.split(",")
       yield Type, 1

    def reducer(self,Type,occurence):
        yield Type,sum(occurence)

if __name__ == '__main__':
    PropertyType.run()






















#plot
import pandas as pd

df= pd.read_csv("Property.csv")
df.columns=['ID','Year','Month','Type','Price']

x = df.groupby('Type')['ID'].count().sort_values()

x.plot(kind='barh',ylabel="Type")




 



