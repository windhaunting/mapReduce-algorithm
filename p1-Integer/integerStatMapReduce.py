#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 19:52:30 2018

@author: fubao
"""


# problem 1 integer:


from mrjob.job import MRJob
from mrjob.step import MRStep

#WORD_RE = re.compile(r"[\w']+")


#(a) The largest integer
# run input1a-01, ....

class MRInteger(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_largest,
                   reducer=self.reducer_get_largest),     
            ]

    def mapper_get_largest(self, _, line):
        
        for inte in line.split():
            yield 1, int(inte.strip())
        #yield "integer", len(line.split())
    
    def reducer_get_largest(self, key, values):
        yield "the largest integer: ", (max(values))              # max(values) indicates the largest integer
        #yield key, values


#(b) The average of all the integers
# run input1b-01, ....

class MRAverage(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_average,
                   reducer=self.reducer_get_average)  
            ]

    def mapper_get_average(self, _, line):
        
        for inte in line.split():
            yield 1, int(inte.strip())
        #yield "integer", len(line.split())
    
    def reducer_get_average(self, key, Integer):
        cnt = v = 0
        for i in Integer:
            cnt += 1          # sum of count
            v += i            # sum of integer
        yield "average of these integers :", v/cnt      # 
        #yield 

#(c) The same set of integers, but with each integer appearing only once
# run input1c-01, ....

class MRSameInteger(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_SameInteger,
                   reducer=self.reducer_get_SameInteger)  
            ]

    def mapper_get_SameInteger(self, _, line):
        
        for inte in line.split():
            yield int(inte.strip()), 1
        #yield "integer", len(line.split())
    

    def reducer_get_SameInteger(self, key, value):
        yield key, 1     # only appear once
        
        
#(d) The count of the number of distinct integers in the input
# run input1d-01, .... 1 2 3 3
class MRCountDistinct(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_countDistinct,
                   reducer=self.reducer_get_countDistinct),
           MRStep(reducer=self.reducer_get_final_countDistinct)
            ]
    '''
    def mapper_get_countDistinct(self, _, line):
        
        for inte in line.split():
            yield 1, int(inte.strip())
        #yield "integer", len(line.split())
    
    def reducer_get_countDistinct(self, key, values):
        yield key, sum(values)              # max(values) indicates the largest integer
        #yield key, values

    '''
    def mapper_get_countDistinct(self, _, line):
        
        for inte in line.split():
            yield int(inte.strip()), 1
        #yield "integer", len(line.split())
    

    def reducer_get_countDistinct(self, key, value):
        yield min(value), key     # only appear once

    #def mapper_get_keyReverse(self, key, value):
    #    yield key, value
        #yield "integer", len(line.split())
        
    def reducer_get_final_countDistinct(self, key, value):
        cnt = 0
        for i in value:
            cnt += 1
        yield "number of distinct integer: ", cnt        # number of distinct integer
        
if __name__ == '__main__':
    # 1. (a)
     MRInteger.run()
    # 1. (b)
    # MRAverage.run()
    
    # 1. (c)
    # MRSameInteger.run()
    #MRCountDistinct.run()