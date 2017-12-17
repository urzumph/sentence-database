# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.template import loader
from django.template import Template, RequestContext
from .models import Searches
from sent.search import search
from sent import do_add_file
import pickle
import sys
import os
import django_rq

# Create your views here.
#def index(request):
    #return HttpResponse("Hello, world. You're at the sdb index.")
    
basedir = '/home/christian/Code/sdb2'

def inner(request, cxt):
    if cxt == None:
        cxt = {}
    
    searches = Searches.objects.all()
    template = loader.get_template('index.html')
    cxt['searches'] = searches
    cxt['numsearches'] = len(searches)
    #print cxt
    return HttpResponse(template.render(cxt, request))
    #return HttpResponse(template.render(RequestContext(request, cxt)))

def index(request):
    #print "index()"
    return inner(request, None)
    

def addsearch(request):
    newkw = request.POST.get('newkw')
    
    # add db entry
    query = Searches(keyword = newkw)
    query.save()
    sfn = "{}/pickles/{}.pkl".format(basedir, query.id)
    # create search results file
    s = search(sfn, newkw)
    
    # get list of data files
    datafiles = os.listdir("{}/data/".format(basedir))
    
    # enqueue all data file additions
    for d in datafiles:
        django_rq.enqueue(do_add_file, sfn, "{}/data/{}".format(basedir,d))
    
    return inner(request, {'message': "Added kw %s with id %d" % (newkw, query.id)})

def delete(request):
    sid = request.POST.get('sid')
    query = Searches(id = sid)
    query.delete()
    cxt = {}
    
    try:
        os.unlink("{}/pickles/{}.pkl".format(basedir, int(sid)))
        cxt['message'] = "Deleted picklefile with id %d" % int(sid)
    except OSError as e:
        cxt['message'] = str(e)
    
    return inner(request, cxt)

def getsearch(request, sid):
    cxt = {}
    cxt['getsearch'] = 1
    try:
        s = pickle.load(open("{}/pickles/{}.pkl".format(basedir, int(sid)),'rb'), encoding='utf-8')
        try:
            cxt['exclusions'] = s.exclusions
        except AttributeError:
            cxt['exclusions'] = []
        cxt['results'] = s.searchresults
    except IOError as e:
        cxt['message'] = str(e)
    
    cxt['sid'] = sid
    
    return inner(request, cxt)

def addexclusion(request):
    newexcl = request.POST.get('newexcl')
    sid = int(request.POST.get('sid'))
    
    sfn = "{}/pickles/{}.pkl".format(basedir, sid)
    
    try:
        s = pickle.load(open(sfn,'rb'), encoding='utf-8')
    except IOError as e:
        cxt['message'] = str(e)
        return inner(request, cxt)
    
    s.updatefn(sfn)
    s.addexclusion(newexcl)
    
    # get list of data files
    datafiles = os.listdir("{}/data/".format(basedir))
    
    # enqueue all data file additions
    for d in datafiles:
        django_rq.enqueue(do_add_file, sfn, "{}/data/{}".format(basedir,d))
    
    return inner(request, {'message': "Added exclusion %s to id %d. Search will rerun" % (newexcl, sid)})
