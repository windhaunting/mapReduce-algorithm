#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 19:52:30 2018

@author: fubao
"""


# problem 1 integer:


# The largest integer

import re

from mrjob.job import MRJob
from mrjob.step import MRStep


#WORD_RE = re.compile(r"[\w']+")

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
            yield (1, inte.lower())
        #yield "integer", len(line.split())
    
    def combiner_get_average(self, cnt, Integer):
        yield sum(cnt), sum(Integer)             # 
        #yield key, values

    def reducer_get_average(self, cnt, sumInteger):
        yield cnt, sumInteger           # 
        #yield key, values
        
if __name__ == '__main__':
    #MRInteger.run()
    MRAverage.run()
