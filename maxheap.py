#!/usr/bin/env python3

# https://stackoverflow.com/questions/30443150/maintain-a-fixed-size-heap-python
from heapq import heapify, heappush, heappushpop, nlargest

class MaxHeap():

    def __init__(self, top_n):
        self.h = []
        self.maxsize = top_n
        heapify( self.h)

    def add(self, element):
        if len(self.h) < self.maxsize:
            heappush(self.h, element)
        else:
            heappushpop(self.h, element)

    def top(self):
        return nlargest(len(self.h), self.h)
