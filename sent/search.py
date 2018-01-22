#!/usr/bin/python3
# coding=UTF-8

import pickle
import os
import operator
import codecs
import re
import string
from django.conf import settings

# Ideas:
# Exclusions (with reprocessing) : 物議
# Add numbers (both widths) to frequency pickle file
# Penalise long runs of kanji

class search:
    sortedsize = 10000
    maxsize = 20000
    freq = pickle.load(open(os.path.join(settings.BASE_DIR, 'sent', 'freq.pkl'),'rb'), encoding='utf-8')
    
    def sort(self):
        if self.issorted:
            return
        print("*** SORT ***")
        self.searchresults.sort(key=operator.itemgetter('score'))
        self.searchresults = self.searchresults[0:self.sortedsize]
        self.issorted = True
    
    def save(self):
        self.sort()
        fh = open(self.fn, 'wb')
        pickle.dump(self, fh)
        fh.close()
        print("Wrote pickle ", self.fn)
    
    def __init__(self, myfile, kw):
        self.fn = myfile
        self.searchresults = []
        self.searchedfiles = []
        self.exclusions = []
        self.keyword = kw
        self.issorted = True
        self.calcscoredebug = False
        self.save()
    
    def calcscore(self, s):
        total = 0
        
        if self.calcscoredebug:
            print("calcscore(", s, ")")
        
        # Optimal length
        optilen = 20
        lendiff = abs(len(s)-optilen)
        total += lendiff * 30
        if self.calcscoredebug:
            print("Lendiff:", lendiff)
        
        # Contains chars we probably don't want in the string
        unwanted = 0
        unwanted += len(re.findall(r'[A-Za-z@#&=%\-:\s]', s, flags=re.UNICODE))
        total += unwanted * 20
        if self.calcscoredebug:
            print("Unwanted:", unwanted)
        
        # Now we need to do some string manipulation stuff - better make everything unicode
        #try:
        #    us = unicode(s, 'utf-8')
        #except TypeError:
        #    us = s
        #try:
        #    ukw = unicode(self.keyword, 'utf-8')
        #except TypeError:
        #    ukw = self.keyword
        us = s
        ukw = self.keyword

        # Penalise strings with characters that do not occur frequently
        freqtotal = 0
        freqpenalty = 0
        
        for i in range(0, len(us)):
            thisfreq = 0
            utfc = us[i].encode('utf-8')
            try:
                thisfreq = self.freq[utfc]['freq']
                if self.calcscoredebug:
                    print("freq-part: ", utfc, thisfreq)
            except KeyError:
                thisfreq = 10000
                if self.calcscoredebug:
                    print("freq-part: ", utfc, "(lookup failed) 10000")
            freqtotal += thisfreq
        
        freqpenalty = freqtotal/500
        total += freqpenalty
        if self.calcscoredebug:
            print("freq:", freqpenalty)
        
        # Penalise strings with the search word at the start
        startpenalty = 0
        index = us.find(ukw)
        if index == 0:
            startpenalty = 100
        total += startpenalty
        
        if self.calcscoredebug:
            print("startpenalty:", startpenalty)
        
        # Penalise strings which don't change character class before and after keyword
        charclasspenalty = 30
        prechangepenalty = False
        postchangepenalty = False
        
        if index != 0:
            try:
                startcharclass = self.freq[us[index].encode('utf-8')]['class']
                prevcharclass = self.freq[us[index-1].encode('utf-8')]['class']
                if startcharclass == prevcharclass:
                    #print "charclass pre debug:", startcharclass, prevcharclass
                    prechangepenalty = True
            except KeyError:
                # couldn't find one of the character classes because it's not in the list
                # let's assume that's not good and give a penalty anyway
                prechangepenalty = True
        
        eindex = index + len(ukw) - 1
        #print eindex, len(us)
        if eindex +1 != len(us):
            try:
                endcharclass = self.freq[us[eindex].encode('utf-8')]['class']
                aftercharclass = self.freq[us[eindex+1].encode('utf-8')]['class']
                if endcharclass == aftercharclass:
                    #print "charclass post debug:", endcharclass, us[eindex], aftercharclass, us[eindex+1]
                    postchangepenalty = True
            except KeyError:
                # couldn't find one of the character classes because it's not in the list
                # let's assume that's not good and give a penalty anyway
                postchangepenalty = True
        
        classchangetotal = 0
        if prechangepenalty:
            classchangetotal += charclasspenalty
        if postchangepenalty:
            classchangetotal += charclasspenalty
        total += classchangetotal
        
        if self.calcscoredebug:
            print("charclass:", prechangepenalty, postchangepenalty, classchangetotal)
        
        # Do debug print and return
        
        if self.calcscoredebug:
            print("calcscore(", s, ") -> ", total)
        return total
    
    def add(self, s):
        if not self.keyword in s:
            return
        
        for e in self.exclusions:
            if e in s:
                #print(s, "(", type(s), ") contains ", e, "(", type(e), ")")
                return
            #else:
            #    print(s, "(", type(s), ") does not contain ", e, "(", type(e), ")")
        
        #print 'add(', s, ')'
        for es in self.searchresults:
            if es['sentence'] == s:
                return
        
        nar = {}
        nar['sentence'] = s
        nar['score'] = self.calcscore(s)
        
        self.searchresults.append(nar)
        self.issorted = False
        
        if len(self.searchresults) > self.maxsize:
            self.sort()
        
        
    def addfile(self, fn):
        ifh = codecs.open(fn, 'r', 'utf-8')
        last = 0
        size = os.fstat(ifh.fileno()).st_size
        print("Starting addfile run on ", fn, "with exclusions", self.exclusions)
        
        for s in ifh:
            s.strip()
            self.add(s)
            curr = int(float(ifh.tell()) / float(size) * float(100))
            if curr != last:
                print(curr, '%')
                last = curr
        
        ifh.close()
        self.searchedfiles.append(fn)
        self.save()
    
    def updatefn(self, nfn):
        # necessary to migrate pickles from v1 to v2
        self.fn = nfn
        self.save()

    def addexclusion(self, excl):
        self.searchresults = []
        self.searchedfiles = []
        try:
            self.exclusions.append(excl)
        except AttributeError:
            self.exclusions = [excl]
        
        print(self.exclusions)
        
        self.save()
