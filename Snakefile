import os
import bz2
import xml.etree.ElementTree
import re
import pickle
import unicodedata
import jaconv

def ls(dirpath, format):
  toret = []
  for f in os.listdir(path=dirpath):
    if f.endswith(format):
      toret.append(os.path.join(dirpath, f))
  return toret

sep = "\t"
global_limit = 0

rule all:
  # N.B. - needs to be replaced with an indexing / scoring function
  input:
    "processed_data/frequency_analysis/charfreq.pkl",
    "processed_data/output/wikipedia.txt.bz2",
    "processed_data/output/tatoeba.txt.bz2"
  output:
    "data/sdb.txt.bz2"
  run:
    limit = global_limit
    sentcount = 0
    with bz2.open(output[0], mode="wt") as of:
      missingchars = list()
      pfh = open(input[0], mode="rb")
      freq = pickle.load(pfh)
      for infn in input[1:]:
        with bz2.open(infn, mode="rt") as inf:
          for line in inf:
            sentcount += 1
            fc = 0
            line = line.rstrip()
            split = line.split(sep)
            text = split[0]
            note = split[1]
            for c in text:
              try:
                #print(c, freq[c])
                if freq[c][1] is not None:
                  fc += freq[c][1]
              except KeyError:
                #print(c, "Not in freq file")
                missingchars.append(c)
            of.write("{}{}{}{}{}\n".format(text, sep, note, sep, fc))
            if limit != 0 and sentcount > limit:
              break
          of.write("Missing Chars: {}".format(missingchars))

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
    phonetic = re.compile(r'{{IPA', flags=re.IGNORECASE)
    # Correct / non-broken ref tag pairs
    ref = re.compile(r'<ref[^<]*</ref>')
    # Self-closing ref tag
    refshort = re.compile(r'<ref[^/]*/>')
    # ref tag with no end
    refend = re.compile(r'<ref(>| name| group)[^<]*$')
    # ref tag with no start
    reflonelyclose = re.compile(r'</ref>')
    # Temporary links to another language
    tlink = re.compile(r'{{仮リンク\|([^\|]+)\|[^\|]*\|[^}]*}}')
    # Image link
    ilink = re.compile(r'\[\[(ファイル|File):[^\]]*\]\]')
    # nbsp
    nbsp = re.compile(r'&nbsp;')
    # html comments
    htmlc = re.compile(r'<!--.*?-->')
    # citations
    cite = re.compile(r'{{Cite[^}]*}}', flags=re.IGNORECASE)
    # English language links
    ell = re.compile(r'{{lang\|en\|([^}]+)}}', flags=re.IGNORECASE)
    sell = re.compile(r'{{Lang-en\|([^}]+)}}', flags=re.IGNORECASE)
    # Non-english language links
    snell = re.compile(r'{{lang-[a-zA-Z]+\|', flags=re.IGNORECASE)
    nell = re.compile(r'{{lang\|[a-zA-Z]+\|([^}]+)}}', flags=re.IGNORECASE)
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
              usent = sent
              # Replace en lang links
              usent = ell.sub(r'\1', usent)
              usent = sell.sub(r'\1', usent)
              # Delete sentences still containing lang links since they're non-english
              if snell.search(usent) or nell.search(usent):
                continue
              # left-strip markers for lists, table formatting, indentation, etc
              usent = usent.lstrip('#*:|-! ')
              usent = headre.sub(r'\1', usent)
              usent = boldital.sub(r'\1', usent)
              usent = tags.sub("", usent)
              usent = complexlinks.sub(r'\1', usent)
              usent = simplelinks.sub(r'\1', usent)
              usent = tlink.sub(r'\1', usent)
              usent = ilink.sub('', usent)
              usent = nbsp.sub(' ', usent)
              usent = htmlc.sub('', usent)
              usent = cite.sub('', usent)
              usent = ref.sub('', usent)
              usent = refshort.sub('', usent)
              usent = refend.sub('', usent)
              usent = reflonelyclose.sub('', usent)

              # re-check length, it may be shorter now after regex changes
              if len(usent) > 5 and sanitycheck.search(usent):
                of.write("{}{}{}\n".format(usent, sep, link))
              if sentcount % 100000 == 0:
                print("{} sentences processed".format(sentcount))


rule wikipedia_noxml:
  input:
    ls("raw_data/wikipedia/", "bz2")
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
          titleskip = False
          for event, elem in xml.etree.ElementTree.iterparse(inf):
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            # Grab the text out of the title, and add it in a wikipedia link
            # for "context" of the sentence
            if elem.tag.endswith("}title"):
              titleskip = False
              title = elem.text
              titlecount += 1
              if titlecount % 10000 == 0:
                print("{} titles complete".format(titlecount))
              # Skip articles which contain rote phrases
              if title.startswith("Wikipedia:アップロードログ"):
                titleskip = True
            if not titleskip and elem.tag.endswith("}text") and elem.text is not None:
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
                of.write("{}{}[[{}]]\n".format(stripped, sep, title))
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
        senhash = dict()
        interestingids = set()
        engidhash = dict()
        print("Generating input dictionary")
        with bz2.open(input[0], mode="rt") as inf:
          for line in inf:
            limit -= 1
            if limit <= 0 and global_limit != 0:
              return
            stripped = line.strip()
            splited = stripped.split(sep)
            sent = splited[0]
            if len(splited) == 1:
              idt = []
            else:
              idt = splited[1].split(' ')
            ids = []
            #print(splited, ids)
            for it in idt:
              text = it.strip()
              if len(text) > 0:
                intid = int(text)
                engidhash[intid] = sent
                interestingids.add(int(text))
            senhash[sent] = None

        # fkidhash should now be <translation id> => sentence
        # here, we transform it to sentence => translation
        print("Replacing ids in memory")
        for senl in senf:
          senstrip = senl.strip()
          sensplit = senstrip.split("\t")
          senid = int(sensplit[0])
          if senid not in interestingids:
            continue
          if sensplit[1] != "eng":
            continue
          engsent = sensplit[2]
          senhash[engidhash[senid]] = engsent

        # lastly, do the actual writing
        print("Writing output to file")
        for k, v in senhash.items():
          if type(v) is not str:
            of.write("{}{}{}\n".format(k, sep, "(No English Translation)"))
          else:
            of.write("{}{}{}\n".format(k, sep, v))

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
      with bz2.open(input[0], mode="rt") as inf:
        # First loop: collect all required ids in dict
        for line in inf:
          splited = line.split(sep)
          targetid = splited[1].strip()
          links[targetid] = []
          
      # For each link, add it to appropriate array of links
      with bz2.open(input[1], mode="rt") as linkf:
        for linkl in linkf:
          linksplit = linkl.split("\t")
          # links is <id> \t <other sentence id>
          if linksplit[0] in links:
            targetid = linksplit[1].strip()
            links[linksplit[0]].append(targetid)

      # Now, go back to the start of the input sentences
      with bz2.open(input[0], mode="rt") as inf:
        # And for each input sentences, re-print with the
        # id list we collected from the links file
        for line in inf:
          splited = line.split(sep)
          targetid = splited[1].strip()
          of.write("{}{}{}\n".format(splited[0], sep, ' '.join(links[targetid])))

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
              text = splited[2].strip()
              of.write("{}{}{}\n".format(text, sep, splited[0]))

rule tatoeba_extract_sent:
  # Remove the tarfile wrapper that the tatoeba guys included
  # Also, strip everything except Japanese (for study) and English (for translation)
  input:
    "raw_data/tatoeba/sentences.tar.bz2"
  output:
    "processed_data/tatoeba_extract/sentences.csv.bz2"
  shell:
    "tar -xOjf {input} sentences.csv | egrep '(eng|jpn)' | bzip2 > {output}"

rule tatoeba_extract_links:
  # Remove the tarfile wrapper that the tatoeba guys included
  input:
    "raw_data/tatoeba/links.tar.bz2"
  output:
    "processed_data/tatoeba_extract/links.csv.bz2"
  shell:
    "tar -xOjf {input} links.csv | bzip2 > {output}"

rule frequency_pickle:
  input:
    "raw_data/frequency_analysis/charfreq.txt"
  output:
    "processed_data/frequency_analysis/charfreq.pkl"
  run:
    with open(output[0], mode="wb") as of:
      with open(input[0], mode="rt") as inf:
        symbols = '：:；;() {}[]|/\\<>,.＆&"\'「」'
        freq = dict()
        count = 1
        line = inf.readline()
        # First line is ignored
        # Read through input file
        while line or exception:
          count += 1
          exception = False
          try:
            line = inf.readline()
          except UnicodeDecodeError:
            print("Unicode Decode Error on line {}".format(count))
            exception = True
          if exception:
            continue
          line = line.rstrip()
          if len(line) == 0:
            continue
          splitline = line.split("\t")
          # Input file contains character type, character itself, and frequency count
          ctype = splitline[0]
          char = splitline[1]
          freqcount = int(splitline[2])
          # Create a lookup dictionary
          freq[char] = [ctype, freqcount]

        # Add normalized versions for all characters in the dictionary
        for k, v in freq.copy().items():
          # unicodedata normalization
          alternatekey = unicodedata.normalize('NFKC', k)
          if alternatekey != k:
            print("(unicodedata) {} = {}".format(k, alternatekey))
            if alternatekey in freq:
              print("Warning: (unicodedata) Multiple code points simplify to {}".format(k))
            else:
              freq[alternatekey] = v

          # full -> half width conversion
          alternatekey = jaconv.z2h(k, kana=True, digit=True, ascii=True)
          if alternatekey != k:
            print("(jaconv) {} = {}".format(k, alternatekey))
            if alternatekey in freq:
              print("Warning: (jaconv) Multiple code points simplify to {}".format(k))
            else:
              freq[alternatekey] = v

          # upper case
          hkey = jaconv.z2h(k, kana=True, digit=True, ascii=True)
          alternatekey = hkey.upper()
          if alternatekey != hkey:
            print("(upper) {} = {}".format(k, alternatekey))
            if alternatekey in freq:
              print("Warning: (upper) Multiple code points simplify to {}".format(k))
            else:
              freq[alternatekey] = v

        for c in symbols:
          if c not in freq:
            freq[c] = ["Symbol", None]

        pickle.dump(freq, of)
