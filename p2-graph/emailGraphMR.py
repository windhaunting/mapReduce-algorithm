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
            yield srcId, -1                # indegree 0

        #yield "integer", len(line.split())
    
    def reducer_get_eachInDegree(self, key, values):
        cnt = 0
        for v in values:
            if v != -1:
                cnt += 1
        yield key, cnt
    
    def mapper_get_averageInDegree(self, key, cnt):
        yield 1, cnt
    
    def reducer_get_averageInDegree(self, key, values):
        # get mean of indegree
        lst = list(values)
        yield "average of indegree :", float(sum(lst))/float(len(lst))      # 
        
        
        # get median of indegree
        medianIndegree = 0
        sortedValues = sorted(lst)
        index = (len(sortedValues) -1) // 2
        #print ("sortedValues: ", sortedValues, index)
        if len(sortedValues) % 2 == 0:
            medianIndegree =  (sortedValues[index] + sortedValues[index+1])/2.0
        else:
            medianIndegree = sortedValues[index]
            
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
            yield srcId, dstId              #outdegree
            yield dstId, -1                 # outdegree 0

    
    def reducer_get_eachOutdegree(self, key, values):
        cnt = 0
        for v in values:
            if v != -1:
                cnt += 1
        yield key, cnt
    
    def mapper_get_averageOutdegree(self, key, cnt):
        yield 1, cnt
    
    def reducer_get_averageOutdegree(self, key, values):
        # get mean of indegree
        lst = list(values)
        sortedValues = sorted(lst)
        yield "average of outdegree :", float(sum(lst))/float(len(lst))      # 
        
        
        # get median of indegree
        medianOutdegree = 0
        index = (len(sortedValues) -1) // 2
        #print ("sortedValues: ", sortedValues, index)
        
        if len(sortedValues) % 2 == 0:
            medianOutdegree =  (sortedValues[index] + sortedValues[index+1])/2.0
        else:
            medianOutdegree = sortedValues[index]
            
        yield "median of outdegree :", medianOutdegree 


# • Average (and median) number of nodes reachable in two hops [15]

class MRTwohops(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_twoHopsFirst,
                   reducer=self.reducer_get_twoHopsFirst),  
            #MRStep(mapper=self.mapper_get_twoHopsSecond,
            #       reducer=self.reducer_get_twoHopsSecond), 
            ]

    def mapper_get_twoHopsFirst(self, _, line):
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
            yield srcId, ([], [dstId])                 #values:  (indegree, outDegree)
            yield dstId, ([srcId], [])                 
    
    def reducer_get_twoHopsFirst(self, key, values):
    
        #yield key, list(values)
        
        inDegrees = []
        outDegrees = []
        inDegrees += list(values)[0]
        outDegrees = list(values)[1]
        yield key, (inDegrees, outDegrees)
        
    def mapper_get_twoHopsSecond(self, key, cnt):
        yield key, cnt
    
    def reducer_get_twoHopsSecond(self, key, values):
        # get avearge of nodes in two hops
       
        yield key, values 



# Average (and median) number of nodes reachable in two hops

if __name__ == '__main__':
     # 2. Number of nodes in the graph 
     #MRNumberNode.run()
     
     # Average (and median) indegree and out degree [10]
     #MRAverageMedianInDegree.run()
     #MRAverageMedianOutDegree.run()
     
     #• Average (and median) number of nodes reachable in two hops [15]
     MRTwohops.run()
     