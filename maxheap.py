#!/usr/bin/env python3

# https://stackoverflow.com/questions/30443150/maintain-a-fixed-size-heap-python
from heapq import heapify, heappush, heappop, heappushpop, nlargest

class MaxHeap():

    def __init__(self, top_n):
        self.h = []
        self.maxsize = top_n
        heapify(self.h)

    def add(self, element):
        if len(self.h) < self.maxsize:
            heappush(self.h, element)
        else:
            heappushpop(self.h, element)

    def top(self, count=None):
        if not count: count = len(self.h)
        return nlargest(count, self.h)

    def get_list(self):
        return self.h

    def reset(self, new_h, top_n=None):
        if new_h:
            if top_n:
                self.maxsize = top_n
            else:
                self.maxsize = len(new_h)
                self.h = new_h
                heapify(self.h)
                while len(self.h) > self.maxsize:
                    heappop(self.h)
        else:
            self.h = []
            self.maxsize = None
        return
