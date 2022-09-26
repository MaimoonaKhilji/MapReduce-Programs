from mrjob.job import MRJob
#mapreduce

class ShoesPerYear(MRJob):
   
    def mapper(self, key, line):
      InvoiceNo,Date,Country,ProductID,Shop,Gender,Size,\
      UnitPrice,Discount,Year,Month,SalePrice = line.split(",")
      yield Year, 1

    def reducer(self,Year,Occurrence):
        yield Year,sum(Occurrence)
        
if __name__ == '__main__':
    ShoesPerYear.run()



