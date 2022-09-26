from mrjob.job import MRJob


class QuantityPerRegion(MRJob):
    
     def mapper(self, key, line):
        OrderDate,Region,City,Category,Product,Quantity,UnitPrice,TotalPrice,\
            Payable_tax,Total_Payable_amount = line.split(",")
        yield (Region,float(Quantity))
        
     def reducer(self,key,values):
         
        yield key, (sum(values))

if __name__ == '__main__':
    QuantityPerRegion.run()
    

