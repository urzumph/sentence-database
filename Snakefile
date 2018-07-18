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
    # Sentence split regex
    # Full-width full stop
    # || Wikipedia markup table column separators
    # full stop + space, because full stop alone causes
    # false positives when dealing with URLs
    sentsplit = re.compile('([。]|\|\||\. )')
    # All full sentences (not phrases / clauses)
    # should contain one of the major particles
    sanitycheck = re.compile('[をがは]')
    # Wiki-markup headings
    headre = re.compile('^={1,6}([^=]*)={1,6}$')
    # Wiki-markup bold/italics
    boldital = re.compile("'{2,5}([^']*)'{2,5}")
    # Random tags that seem to show up in some articles
    tags = re.compile("</?(em|li)>")
    # Wiki-markup [[Page]] links
    simplelinks = re.compile("\[\[([^\[\]\|]*)\]\]")
    # Wiki-markup [[Page|Displayed Text]] links
    complexlinks = re.compile(r'\[\[[^\[\]\|]+\|([^\[\]\|]+)\]\]')
    # IPA phonetics
    phonetic = re.compile(r'{{IPA')
    # Correct / non-broken ref tag pairs
    ref = re.compile(r'<ref[^<]*</ref>')
    # Self-closing ref tag
    refshort = re.compile(r'<ref[^/]*/>')
    # ref tag with no end
    refend = re.compile(r'<ref(>| name| group)[^<]*$')
    # ref tag with no start
    reflonelyclose = re.compile(r'</ref>')
    # Temporary links to another language
    tlink = re.compile(r'{{仮リンク|([^\|]+)|[^\|]*|[^}]*}}')
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with bz2.open(infn, mode="rt") as inf:
          for line in inf:
            # This limit is so we can test with small amounts of input initially
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            # line should be <text block>\t<url>
            parts = line.split(sep)
            link = parts[1].rstrip()
            # Split based on the defined sentence separators
            # to turn <text block> into one or more sentences
            strarr = sentsplit.split(parts[0])

            for sent in strarr:
              sentcount += 1
              # Strip out sentences with IPA - I can't read the damn things
              if phonetic.search(sent):
                continue
              # left-strip markers for lists, table formatting, indentation, etc
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

              # re-check length, it may be shorter now after regex changes
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
    sanitycheck = re.compile('[をがは]')
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with bz2.open(infn, mode="rt") as inf:
          # Need to use the iterative mode of etree
          # The XML is so big it will exhaust all available
          # memory unless your PC is huge
          for event, elem in xml.etree.ElementTree.iterparse(inf):
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            # Grab the text out of the title, and add it in a wikipedia link
            # for "context" of the sentence
            if elem.tag.endswith("}title"):
              title = elem.text
              titlecount += 1
              if titlecount % 10000 == 0:
                print("{} titles complete".format(titlecount))
            if elem.tag.endswith("}text") and elem.text is not None:
              lines = elem.text.split("\n")
              for line in lines:
                # We need to ensure that the original text does not contain
                # any of the separator we plan to use for our text files
                notabs = line.replace(sep, " ")
                stripped = notabs.strip()
                if len(stripped) < 5:
                  # No way a valid sentence is < 5 chars
                  continue
                if not sanitycheck.search(stripped):
                  # Analysis of the output text seemed to show a lot
                  # which were failing the sanity check. The check
                  # is simple and hopefully this should reduce the size
                  # of the text to store on disk. 
                  continue
                of.write("{}{}https://ja.wikipedia.org/wiki/{}\n".format(stripped, sep, title))
            elem.clear()

rule tatoeba_idreplace:
  # tatoeba-to is <text> \t <space separated list of ids>
  # sentences is <id> \t <lang> \t <text>
  input:
    "processed_data/tatoeba_link/tatoeba-to.txt.bz2",
    "processed_data/tatoeba_extract/sentences.csv.bz2"
  output:
    "processed_data/output/tatoeba.txt.bz2"
  run:
    limit = global_limit
    with bz2.open(output[0], mode="wt") as of:
      with bz2.open(input[1], mode="rt") as senf:
        with bz2.open(input[0], mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            senf.seek(0)
            splited = line.split(sep)
            ids = splited[1].split(' ')
            idindex = 0
            written = False
            # Look through sentences trying to find one with the correct id
            # to replace the id list. The links list will include ids that
            # don't occur in the sentences list, because they are not jpn/eng
            # and were removed previously.
            for senl in senf:
                sensplit = senl.split("\t")
                while sensplit[0] > ids[idindex]:
                  idindex += 1
                  if idindex >= len(ids):
                    break
                if idindex >= len(ids):
                  break
                if sensplit[0] == ids[idindex] and sensplit[1] == "eng":
                  # print <text> \t <linked eng sentence text>
                  of.write("{}{}{}".format(splited[0], sep, sensplit[2]))
                  written = True
                  break
            
            if not written:
              of.write("{}{}{}".format(splited[0], sep, "(No English Translation)"))

rule tatoeba_tolink:
  # tatoeba-from is <text> \t <sentence id>
  # links is <id> \t <other sentence id>
  input:
    "processed_data/tatoeba_link/tatoeba-from.txt.bz2",
    "processed_data/tatoeba_extract/links.csv.bz2"
  # output format should be:
  # <text> \t <space separated list of other sentence ids>
  output:
    "processed_data/tatoeba_link/tatoeba-to.txt.bz2"
  run:
    with bz2.open(output[0], mode="wt") as of:
      links = dict()
      with open(input[0], mode="rt") as inf:
        # First loop: collect all required ids in dict
        for line in inf:
          splited = line.split(sep)
          targetid = splited[1]
          links[targetid] = []
          
      # For each link, add it to appropriate array of links
      with bz2.open(input[1], mode="rt") as linkf:
        for linkl in linkf:
          linksplit = linkl.split("\t")
          # links is <id> \t <other sentence id>
          if linksplit[0] in links:
            links[linksplit[0]].append(linksplit[1])

      # Now, go back to the start of the input sentences
      inf.seek(0)
      # And for each input sentences, re-print with the
      # id list we collected from the links file
      for line in inf:
        splited = line.split(sep)
        of.write("{}{}{}\n".format(splited[0], sep, ' '.join(links[splited[1]])))

rule tatoeba_fromlink:
  input:
    "processed_data/tatoeba_extract/sentences.csv.bz2"
  output:
    "processed_data/tatoeba_link/tatoeba-from.txt.bz2"
  run:
    limit = global_limit
    with bz2.open(output[0], mode="wt") as of:
      for infn in input:
        with bz2.open(infn, mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            # Input sentence files have the format:
            # Sentence id [tab] Lang [tab] Text
            splited = line.split("\t")
            if splited[1] != "jpn":
              continue
            else:
              # We need to output format:
              # <text> \t <sentence id>
              # Sentence id is meaningless now, but it will be transformed
              of.write("{}{}{}\n".format(splited[2], sep, splited[0]))

rule tatoeba_extract_sent:
  # Remove the tarfile wrapper that the tatoeba guys included
  # Also, strip everything except Japanese (for study) and English (for translation)
  input:
    "raw_data/tatoeba/sentences.tar.bz2"
  output:
    "processed_data/tatoeba_extract/sentences.csv.bz2"
  shell:
    "tar -xOjf {input} `basename {output}` | egrep '(eng|jpn)' | bzip2 > {output}"

rule tatoeba_extract_links:
  # Remove the tarfile wrapper that the tatoeba guys included
  input:
    "raw_data/tatoeba/links.tar.bz2"
  output:
    "processed_data/tatoeba_extract/links.csv.bz2"
  shell:
    "tar -xOjf {input} `basename {output}` | bzip2 > {output}"
