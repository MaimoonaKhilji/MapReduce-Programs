from mrjob.job import MRJob
#mapreduce

class DiscountPerCountry(MRJob):
   
    def mapper(self, key, line):
      InvoiceNo,Date,Country,ProductID,Shop,Gender,Size,\
      UnitPrice,Discount,Year,Month,SalePrice = line.split(",")
      Disc=Discount.replace("%", "", 1)
      yield Country, int(float(Disc))

    def reducer(self, Country,Disc):
        total = 0
        numElements = 0
        for x in Disc:
            total += x
            numElements += 1
            
        yield Country, round(total / numElements)
        
if __name__ == '__main__':
    DiscountPerCountry.run()



