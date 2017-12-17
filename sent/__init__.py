from .search import search
import pickle

def do_add_file(sfn, dfn):
    s = pickle.load(open(sfn,'rb'), encoding='utf-8')
    s.addfile(dfn)