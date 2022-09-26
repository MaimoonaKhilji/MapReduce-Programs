# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 21:47:21 2021

@author: Maimoona Khilji
"""

def max(a):
    m=0
    for i in a:
        if(i>m):
            m=i
    return m

a=max([1,2,3,4,5])
print(a)
