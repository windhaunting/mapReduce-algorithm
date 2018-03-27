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

    def mapper_get_numberNode(self, _, line):
        lines = line.strip().split('\t')
        #print ('linessssssss: ', line)  
        #for ids in lines:
        try:
            srcId = int(lines[0])
            dstId = int(lines[1])
        except:
            #print("skipping line with value", lines)
            pass
        else:
            yield srcId, 1
            yield dstId, 1

        #yield "integer", len(line.split())
    
    def reducer_get_numberNode(self, key, values):
        yield 1, key
    
    def reducer_get_final_numberNode(self, key, values):
        cnt = 0
        for i in values:
            cnt += 1
        yield "number of nodes: ", cnt              # len(values) indicates the largest integer
  
        

#• Average (and median) indegree and out degree [10]
# indegree
class MRAverageMedianInDegree(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_InputInDegree,
                   reducer=self.reducer_get_eachInDegree),  
            MRStep(mapper=self.mapper_get_averageInDegree,
                   reducer=self.reducer_get_averageInDegree), 
            ]

    def mapper_get_InputInDegree(self, _, line):
        lines = line.strip().split('\t')
        #print ('linessssssss: ', line)  
        #for ids in lines:
        try:
            srcId = int(lines[0])
            dstId = int(lines[1])
        except:
            #print("skipping line with value", lines)
            pass
        else:
            yield dstId, srcId              #indegree
            #yield srcId, dstId, srcId

        #yield "integer", len(line.split())
    
    def reducer_get_eachInDegree(self, key, values):
        cnt = 0
        for i in values:
            cnt += 1
        yield key, cnt
    
    def mapper_get_averageInDegree(self, key, cnt):
        yield 1, cnt
    
    def reducer_get_averageInDegree(self, key, values):
        # get mean of indegree
        lst = list(values)
        # get median of indegree
        medianIndegree = 0
        sortedValues = sorted(lst)
        index = (len(sortedValues) -1) // 2
        print ("sortedValues: ", sortedValues, index)
        if len(sortedValues) % 2 == 0:
            medianIndegree =  (sortedValues[index] + sortedValues[index+1])/2.0
        else:
            medianIndegree = sortedValues[index]
            
            
        yield "average of indegree :", float(sum(lst))/float(len(lst))      # 
        yield "median of indegree :", medianIndegree 

#out degree
class MRAverageMedianOutDegree(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_InputOutdegree,
                   reducer=self.reducer_get_eachOutdegree),  
            MRStep(mapper=self.mapper_get_averageOutdegree,
                   reducer=self.reducer_get_averageOutdegree), 
            ]

    def mapper_get_InputOutdegree(self, _, line):
        lines = line.strip().split('\t')
        #print ('linessssssss: ', line)  
        #for ids in lines:
        try:
            srcId = int(lines[0])
            dstId = int(lines[1])
        except:
            #print("skipping line with value", lines)
            pass
        else:
            #yield dstId, srcId              #indegree
            yield srcId, dstId              #outdegree

        #yield "integer", len(line.split())
    
    def reducer_get_eachOutdegree(self, key, values):
        cnt = 0
        for i in values:
            cnt += 1
        yield key, cnt
    
    def mapper_get_averageOutdegree(self, key, cnt):
        yield 1, cnt
    
    def reducer_get_averageOutdegree(self, key, values):
        # get mean of indegree
        #print ("list:ddd ", list(values))
        #lstOutDegree = list(values)[::]
        # get median of indegree
        lst = list(values)
        # get median of indegree
        sortedValues = sorted(lst)
        
        medianOutdegree = 0
        print ("lstOutDegree: ", sortedValues)
        index = (len(sortedValues) -1) // 2
        print ("sortedValues: ", sortedValues, index)
        if len(sortedValues) % 2 == 0:
            medianOutdegree =  (sortedValues[index] + sortedValues[index+1])/2.0
        else:
            medianOutdegree = sortedValues[index]
            
        yield "average of outdegree :", float(sum(sortedValues))/float(len(sortedValues))      # 
        yield "median of outdegree :", medianOutdegree 


if __name__ == '__main__':
    # 2. Number of nodes in the graph 
     #MRNumberNode.run()
     
     #MRAverageMedianInDegree.run()
     MRAverageMedianOutDegree.run()