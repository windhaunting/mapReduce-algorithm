#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 19:52:30 2018

@author: fubao
"""


# problem 1 integer:



import re

from mrjob.job import MRJob
from mrjob.step import MRStep

#WORD_RE = re.compile(r"[\w']+")


#(a) The largest integer

class MRInteger(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_largest,
                   reducer=self.reducer_get_largest),     
            ]

    def mapper_get_largest(self, _, line):
        
        for inte in line.split():
            yield (1, int(inte.lower()))
        #yield "integer", len(line.split())
    
    def reducer_get_largest(self, key, values):
        yield key, (max(values))              # max(values) indicates the largest integer
        #yield key, values



#(b) The average of all the integers
class MRAverage(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_average,
                   combiner=self.combiner_get_average,
                   reducer=self.reducer_get_average)  
            #MRStep(reducer=self.reducer_get_average)   

            ]

    def mapper_get_average(self, _, line):
        
        for inte in line.split():
            yield 1, (1, int(inte.lower()))
        #yield "integer", len(line.split())
    
    def combiner_get_average(self, cnt, Integer):
        yield cnt, sum(Integer[0])             # 

    def reducer_get_average(self, cnt, sumInteger):
        yield cnt, sum(sumInteger)           # 
        #yield 
        
if __name__ == '__main__':
    #MRInteger.run()
    MRAverage.run()
