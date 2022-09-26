from mrjob.job import MRJob
#mapreduce

class ShoeSoldByEachCountry(MRJob):
   
    def mapper(self, key, line):
      InvoiceNo,Date,Country,ProductID,Shop,Gender,Size,\
      UnitPrice,Discount,Year,Month,SalePrice = line.split(",")
      yield Country,1

    def reducer(self,Country,occur):
        yield Country,sum(occur)
        
  
if __name__ == '__main__':
    ShoeSoldByEachCountry.run()

