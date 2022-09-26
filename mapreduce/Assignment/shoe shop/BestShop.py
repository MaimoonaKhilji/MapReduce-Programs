from mrjob.job import MRJob
#mapreduce

class Shops(MRJob):
   
    def mapper(self, key, line):
      InvoiceNo,Date,Country,ProductID,Shop,Gender,Size,\
      UnitPrice,Discount,Year,Month,SalePrice = line.split(",")
      yield Gender, 1

    def reducer(self,Gender,Occurrence):
        yield Gender,sum(Occurrence)
        
if __name__ == '__main__':
    Shops.run()



