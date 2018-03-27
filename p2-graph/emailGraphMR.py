#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 19:37:44 2018

@author: fubao
"""

#email graph 



from mrjob.job import MRJob
from mrjob.step import MRStep

#  • Number of nodes in the graph 

class MRGraph(MRJob):

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

