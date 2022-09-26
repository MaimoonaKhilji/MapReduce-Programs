from mrjob.job import MRJob


class AverageTax(MRJob):
    
    def mapper(self, key, line):
        OrderDate,Region,City,Category,Product,Quantity,UnitPrice,TotalPrice,\
            Payable_tax,Total_Payable_amount = line.split(",")
        yield City, float(Payable_tax)

    def reducer(self,City,tax):
        num=0
        total=0
        for i in tax:
            num=num+1
            total=total+i
            
        yield City,round(total/num)

if __name__ == '__main__':
    AverageTax.run()
    
    
    
    
    