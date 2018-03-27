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

class MRNumberNode(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_numberNode,
                   reducer=self.reducer_get_numberNode),   
            MRStep(reducer=self.reducer_get_final_numberNode)
            ]

    def mapper_get_largest(self, _, line):
        
        for inte in line.split():
            yield inte.strip('\t')[0], 1
            yield inte.strip('\t')[1], 1

        #yield "integer", len(line.split())
    
    def reducer_get_numberNode(self, key, values):
        yield values, key
    
    def reducer_get_final_numberNode(self, key, values):
        yield "number of nodes: ", (sum(values))              # max(values) indicates the largest integer
        #yield key, values

