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
global_limit = 0

rule all:
  # N.B. - needs to be replaced with an indexing / scoring function
  input:
    "processed_data/output/wikipedia.txt.bz2",
    "processed_data/output/tatoeba.txt.bz2"
  output:
    "data/sdb.txt.bz2"
  shell:
    "bzcat {input} | bzip2 > {output}"

rule wikipedia_finish:
  # N.b. Dodgy hack till I figure out the required steps
  input:
    "processed_data/wikipedia_nomarkup/wikipedia.txt.bz2"
  output:
    "processed_data/output/wikipedia.txt.bz2"
  shell:
    "cp {input} {output}"

rule wikipedia_nomarkup:
  input:
    "processed_data/wikipedia_noxml/wikipedia.txt.bz2"
  output:
    "processed_data/wikipedia_nomarkup/wikipedia.txt.bz2"
  run:
    limit = global_limit
    sentcount = 0
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
    tlink = re.compile(r'{{仮リンク|([^\]+)|[^|]*|[^}]*}}')
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
              sentcount += 1
              if phonetic.search(sent):
                continue
              usent = sent.lstrip('#*:|-! ')
              usent = headre.sub(r'\1', usent)
              usent = boldital.sub(r'\1', usent)
              usent = tags.sub("", usent)
              usent = complexlinks.sub(r'\1', usent)
              usent = simplelinks.sub(r'\1', usent)
              usent = tlink.sub(r'\1', usent)
              usent = ref.sub('', usent)
              usent = refshort.sub('', usent)
              usent = refend.sub('', usent)
              usent = reflonelyclose.sub('', usent)

              if len(usent) > 5 and sanitycheck.search(usent):
                of.write("{}{}{}\n".format(usent, sep, link))
              if sentcount % 10000 == 0:
                print("{} sentences processed")


rule wikipedia_noxml:
  input:
    ls("raw_data/wikipedia/")
  output:
    "processed_data/wikipedia_noxml/wikipedia.txt.bz2"
  run:
    limit = global_limit
    title = ""
    titlecount = 0
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with bz2.open(infn, mode="rt") as inf:
          for event, elem in xml.etree.ElementTree.iterparse(inf):
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            if elem.tag.endswith("}title"):
              title = elem.text
              titlecount += 1
              if titlecount % 10000 == 0:
                print("{} titles complete".format(titlecount))
            if elem.tag.endswith("}text") and elem.text is not None:
              lines = elem.text.split("\n")
              for line in lines:
                notabs = line.replace("\t", " ")
                stripped = notabs.strip()
                if len(stripped) < 5:
                  # No way a valid sentence is < 5 chars
                  continue
                of.write("{}{}https://ja.wikipedia.org/wiki/{}\n".format(stripped, sep, title))
            elem.clear()

rule tatoeba_idreplace:
  input:
    "processed_data/tatoeba_link/tatoeba-to.txt.bz2",
    "processed_data/tatoeba_extract/sentences.csv"
  output:
    "processed_data/output/tatoeba.txt.bz2"
  run:
    limit = global_limit
    with bz2.open(output[0], mode="wt") as of:
      with open(input[1], mode="rt") as senf:
        with open(input[0], mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            senf.seek(0)
            splited = line.split(sep)
            ids = splited[1].split(' ')
            idindex = 0
            written = False
            for senl in senf:
                sensplit = senl.split("\t")
                while sensplit[0] > ids[idindex]:
                  idindex += 1
                  if idindex >= len(ids):
                    break
                if idindex >= len(ids):
                  break
                if sensplit[0] == ids[idindex] and sensplit[1] == "eng":
                  of.write("{}{}{}".format(splited[0], sep, sensplit[2]))
                  written = True
                  break
            
            if not written:
              of.write("{}{}{}".format(splited[0], sep, "(No English Translation)"))

rule tatoeba_tolink:
  input:
    "processed_data/tatoeba_link/tatoeba-from.txt.bz2",
    "processed_data/tatoeba_extract/links.csv"
  output:
    "processed_data/tatoeba_link/tatoeba-to.txt.bz2"
  run:
    limit = global_limit
    with bz2.open(output[0], mode="wt") as of:
      with open(input[1], mode="rt") as linkf:
        with open(input[0], mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            linkf.seek(0)
            splited = line.split(sep)
            targetid = splited[1]
            toids = []
            for linkl in linkf:
              linksplit = linkl.split("\t")
              if linksplit[0] == targetid:
                toids.append(linksplit[1])
            of.write("{}{}{}\n".format(splited[0], sep, ' '.join(toids)))

rule tatoeba_fromlink:
  input:
    "processed_data/tatoeba_extract/sentences.csv"
  output:
    "processed_data/tatoeba_link/tatoeba-from.txt.bz2"
  run:
    limit = global_limit
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with open(infn, mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            splited = line.split(sep)
            if splited[1] != "jpn":
              continue
            else:
              of.write("{}{}{}\n".format(splited[2], sep, splited[0]))

rule tatoeba_extract_sent:
  input:
    "raw_data/tatoeba/sentences.tar.bz2"
  output:
    temp("processed_data/tatoeba_extract/sentences.csv")
  shell:
    "tar -xOjf {input} `basename {output}` | egrep '(eng|jpn)' > {output}"

rule tatoeba_extract_links:
  input:
    "raw_data/tatoeba/links.tar.bz2"
  output:
    temp("processed_data/tatoeba_extract/links.csv")
  shell:
    "tar -xOjf {input} `basename {output}` > {output}"
