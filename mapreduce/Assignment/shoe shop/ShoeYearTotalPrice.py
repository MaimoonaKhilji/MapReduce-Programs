from mrjob.job import MRJob
#mapreduce

class ShoeSoldTotal_per_Year(MRJob):
   
    def mapper(self, key, line):
      InvoiceNo,Date,Country,ProductID,Shop,Gender,Size,\
      UnitPrice,Discount,Year,Month,SalePrice = line.split(",")
      yield Year, int(float(SalePrice))

    def reducer(self,Year,SalePrice):
        yield Year,sum(SalePrice)
        
  
if __name__ == '__main__':
    ShoeSoldTotal_per_Year.run()



