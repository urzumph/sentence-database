import os
import bz2
import xml.etree.ElementTree
import re

def ls(dirpath):
  toret = []
  for f in os.listdir(path=dirpath):
    toret.append(os.path.join(dirpath, f))
  return toret

sep = "\t"
global_limit = 100000000

rule wikipedia_nomarkup:
  input:
    "processed_data/wikipedia_noxml/wikipedia.txt.bz2"
  output:
    "processed_data/wikipedia_nomarkup/wikipedia.txt.bz2"
  run:
    limit = global_limit
    sentsplit = re.compile('([。]|\|\||\. )')
    sanitycheck = re.compile('[をがは]')
    headre = re.compile('^={1,6}([^=]*)={1,6}$')
    boldital = re.compile("'{2,5}([^']*)'{2,5}")
    tags = re.compile("</?(em|li)>")
    simplelinks = re.compile("\[\[([^\[\]\|]*)\]\]")
    complexlinks = re.compile(r'\[\[[^\[\]\|]+\|([^\[\]\|]+)\]\]')
    phonetic = re.compile(r'{{IPA')
    ref = re.compile(r'<ref[^<]*</ref>')
    refshort = re.compile(r'<ref[^/]*/>')
    refend = re.compile(r'<ref(>| name| group)[^<]*$')
    reflonelyclose = re.compile(r'</ref>')
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with bz2.open(infn, mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            parts = line.split(sep)
            link = parts[1].rstrip()
            strarr = sentsplit.split(parts[0])

            for sent in strarr:
              if phonetic.search(sent):
                continue
              usent = sent.lstrip('#*:|-! ')
              usent = headre.sub(r'\1', usent)
              usent = boldital.sub(r'\1', usent)
              usent = tags.sub("", usent)
              usent = complexlinks.sub(r'\1', usent)
              usent = simplelinks.sub(r'\1', usent)
              usent = ref.sub('', usent)
              usent = refshort.sub('', usent)
              usent = refend.sub('', usent)
              usent = reflonelyclose.sub('', usent)

              if len(usent) > 5 and sanitycheck.search(usent):
                of.write("{}{}{}\n".format(usent, sep, link))


rule wikipedia_noxml:
  input:
    ls("raw_data/wikipedia/")
  output:
    "processed_data/wikipedia_noxml/wikipedia.txt.bz2"
  run:
    limit = global_limit
    title = ""
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with bz2.open(infn, mode="rt") as inf:
          for event, elem in xml.etree.ElementTree.iterparse(inf):
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            if elem.tag.endswith("}title"):
              title = elem.text
            if elem.tag.endswith("}text") and elem.text is not None:
              lines = elem.text.split("\n")
              for line in lines:
                notabs = line.replace("\t", " ")
                stripped = notabs.strip()
                if len(stripped) < 5:
                  # No way a valid sentence is < 5 chars
                  continue
                of.write("{}{}https://ja.wikipedia.org/wiki/{}\n".format(stripped, sep, title))
