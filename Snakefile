import os
import bz2
import xml.etree.ElementTree
import re
import pickle
import unicodedata
import jaconv
import multiprocessing
import gzip
import html.parser

def ls(dirpath, format):
  toret = []
  for f in os.listdir(path=dirpath):
    if f.endswith(format):
      toret.append(os.path.join(dirpath, f))
  return toret

sep = "\t"
global_limit = 0

def gzinput_read(send_conn, fl, limit):
  sentcount = 0
  for infn in fl:
    with gzip.open(infn, mode="rt") as inf:
      for line in inf:
        sentcount += 1
        if limit != 0 and sentcount > limit:
          send_conn.send(None)
          send_conn.close()
          return
        sline = line.rstrip()
        split = sline.split(sep)
        text = split[0]
        if len(split) > 1:
          note = split[1]
        else:
          note = ''
        if len(split) > 2:
          score = int(split[2])
        else:
          score = 0
        send_conn.send([text, note, score])
  send_conn.send(None)
  send_conn.close()

class Input:
  def __init__(self, fl, limit):
    recv_conn, send_conn = multiprocessing.Pipe(duplex=False)
    self.p = multiprocessing.Process(target=gzinput_read, args=(send_conn, fl, limit))
    self.p.start()
    print("Input Process id: {}".format(self.p.pid))
    send_conn.close()
    self.rc = recv_conn
    self.count = 0

  def next(self):
    self.count += 1
    if self.count % 100000 == 0:
      print("{} lines processed".format(self.count))
    try:
      read = self.rc.recv()
    except EOFError:
      read = None
    if read is None:
      self.text = None
      self.note = None
      self.score = None
      return False
    else:
      self.text = read[0]
      self.note = read[1]
      self.score = read[2]
      return True
 
  def close(self):
    self.rc.close()
    self.p.join()

def gzoutput_write(recv_conn, fn):
  with gzip.open(fn, mode="wt") as of:
    while True:
      try:
        read = recv_conn.recv()
      except EOFError:
        print("gzoutput_write got EOF")
        break
      if read is None:
        print("gzoutput_write got None")
        break
      of.write("{}{}{}{}{}\n".format(read[0], sep, read[1], sep, read[2]))

  print("gzoutput_write closing connection")
  recv_conn.close()
  print("gzoutput_write terminating")

class Output:
  def __init__(self, fn):
    recv_conn, send_conn = multiprocessing.Pipe(duplex=False)
    self.p = multiprocessing.Process(target=gzoutput_write, args=(recv_conn, fn))
    self.p.start()
    self.sc = send_conn
    print("Output Process id: {}".format(self.p.pid))
    recv_conn.close()

  def write(self, obj):
    self.sc.send(obj)

  def close(self):
    self.sc.send(None)
    self.sc.close()
    self.p.join()

rule all:
  input:
    "processed_data/filtered/stage2.txt.gz",
    "processed_data/frequency_analysis/missing_char_analysis.txt"
  output:
    "data/sdb.txt.gz"
  shell:
    "cp {input[0]} {output}"
    #; cat {input[1]} > /dev/null"

rule missing_char_analysis:
  input:
    "processed_data/frequency_analysis/missingchars.txt.gz"
  output:
    "processed_data/frequency_analysis/missing_char_analysis.txt"
  shell:
    "zcat {input} | cut -f 1 | sort | uniq -c | sort -n > {output}"

rule score:
  # N.B. - needs to be replaced with an indexing / scoring function
  input:
    "processed_data/frequency_analysis/charfreq.pkl",
    "processed_data/filtered/stage1.txt.gz"
  output:
    "processed_data/filtered/stage2.txt.gz",
    "processed_data/frequency_analysis/missingchars.txt.gz"
  log:
    "makelog/missingchars.log"
  run:
    limit = global_limit
    missingchars = dict()
    pfh = open(input[0], mode="rb")
    freq = pickle.load(pfh)
    ih = Input(input[1:], limit)
    oh = Output(output[0])
    lh = open(log[0], mode="wt")
    mcfh = Output(output[1])
    while ih.next():
      fc = 0
      for c in ih.text:
        try:
          #print(c, freq[c])
          if freq[c][1] is not None:
            fc += freq[c][1]
        except KeyError:
          #print(c, "Not in freq file")
          if c not in missingchars:
            missingchars[c] = 1
          else:
            missingchars[c] += 1
            cname = unicodedata.name(c, "NONAME")
            mcfh.write(["{} {}".format(c, cname), 
              "{} ({})".format(ih.text, ih.note), missingchars[c]])
            if missingchars[c] == 5:
              lh.write("Missing Char: {} ({}) hit limit of 5\n".format(c, cname))
      
      oh.write([ih.text, ih.note, fc])

    ih.close()
    oh.close()
    mcfh.close()

rule filter:
  input:
    "processed_data/frequency_analysis/charfreq.pkl",
    "processed_data/output/wikipedia.txt.gz",
    "processed_data/output/tatoeba.txt.gz"
  output:
    "processed_data/filtered/stage1.txt.gz"
  log:
    "makelog/stage1_excluded_symbols.txt"
  run:
    limit = global_limit
    sentcount = 0
    # Mostly math stuff that I understand well enough to know would not make useful sentences
    excluded = set()
    excluded.add("∃")
    excluded.add("√")
    excluded.add("∋")
    excluded.add("∈")
    excluded.add("⊂")
    excluded.add("≧")
    excluded.add("⋊")
    excluded.add("≃")
    excluded.add("∩")
    excluded.add("⊃")
    excluded.add("∪")
    excluded.add("∧")
    excluded.add("∀")
    # Known good from pickle file
    included = set()
    pfh = open(input[0], mode="rb")
    freq = pickle.load(pfh)
    for k in freq.keys():
      included.add(k)
    # Bad categories
    badnames = list()
    badnames.append("HANGUL ")
    badnames.append("TAMIL" )
    badnames.append("CYRILLIC ")
    badnames.append("MONGOLIAN ")
    badnames.append("THAI ")
    badnames.append("GUJARATI ")
    badnames.append("MYANMAR ")
    badnames.append("KANNADA ")
    badnames.append("ARMENIAN ")
    badnames.append("LAO ")
    badnames.append("TELUGU ")
    # We check all frequency-sampled Japanese characters
    # before we check bad names. If we hit a CJK character
    # it's either extreemly rare in Japanese or not Japanese
    # either way I'm culling it.
    badnames.append("CJK UNIFIED IDEOGRAPH")
    badnames.append("ETHIOPIC ")
    badnames.append("GURMUKHI ")
    badnames.append("KANGXI ")
    badnames.append("RUNIC LETTER")
    badnames.append("TIFINAGH ")
    badnames.append("ARABIC LETTER")
    badnames.append("ARABIC-INDIC DIGIT")
    badnames.append("HEBREW LETTER")
    # Most stuff doesn't seem to be able to 
    # render these correctly anyway
    badnames.append("NONAME")
    badnames.append("KHMER ")
    badnames.append("TIBETAN ")
    badnames.append("GEORGIAN ")
    # Analysis shows they weren't used in useful sentences
    badnames.append("BOX DRAWINGS ") 
    badnames.append("DEVANAGARI ")
    badnames.append("BOPOMOFO ")
    ih = Input(input[1:], limit)
    oh = Output(output[0])
    # Unicode data to exclude sentences containing languages I can't read
    while ih.next():
      skip = False
      for c in ih.text:
        if c in included:
          continue
        if c in excluded:
          skip = True
          break

        cname = unicodedata.name(c, "NONAME")
        for name in badnames:
          if cname.startswith(name):
            excluded.add(c)
            skip = True
            break
        if skip:
          break 
      if not skip:
        oh.write([ih.text, ih.note, ih.score])

    ih.close()
    oh.close()
    lh = open(log[0], mode="wt")
    lh.write("{}".format(excluded))
    lh.close()

rule wikipedia_finish:
  # N.b. Dodgy hack till I figure out the required steps
  input:
    "processed_data/wikipedia/linesplit.txt.gz"
  output:
    "processed_data/output/wikipedia.txt.gz"
  shell:
    "cp {input} {output}"


rule wikipedia_linesplit:
  input:
    "processed_data/wikipedia/stripchars.txt.gz"
  output:
    "processed_data/wikipedia/linesplit.txt.gz"
  run:
    limit = global_limit
    # Sentence split regex
    # Full-width full stop
    # || Wikipedia markup table column separators
    # full stop + space, because full stop alone causes
    # false positives when dealing with URLs
    sentsplit = re.compile('([。｡]|\|\||\. |%lf%)')
    headre = re.compile('^={1,6}([^=]*)={1,6}$')
    exclude = re.compile('%exclude%')
    # All full sentences (not phrases / clauses)
    # should contain one of the major particles
    sanitycheck = re.compile('[をがは]')
    ih = Input(input, limit)
    oh = Output(output[0])
    while ih.next():
      link = ih.note
      # Split based on the defined sentence separators
      # to turn <text block> into one or more sentences
      strarr = sentsplit.split(ih.text)
      for sent in strarr:
        # Strip sentences with %exclude% tags - HTML parser has decided
        # this sentence is not good
        if exclude.search(sent):
          continue

        usent = sent
        usent = headre.sub(r'\1', usent)
        # left-strip markers for lists, table formatting, indentation, etc
        usent = usent.lstrip('#*:|-! •')
        usent = usent.strip()
        if len(usent) > 5 and sanitycheck.search(usent):
          oh.write([usent, link, ih.score])
    oh.close()
    ih.close()

rule wikipedia_stripchars:
  input:
    "processed_data/wikipedia/nomarkup.txt.gz"
  output:
    "processed_data/wikipedia/stripchars.txt.gz"
  run:
    limit = global_limit
    chars = re.compile(r'(\u200E|\u200B|\u202C|\u202A|\u202D|\u200F|\uFEFF|\u200D|\u200C|\u206C)')
    spaces = re.compile(r'(\u00A0)')
    ih = Input(input, limit)
    oh = Output(output[0])
    while ih.next():
      text = chars.sub('',ih.text)
      text = spaces.sub(' ',text)
      oh.write([text, ih.note, ih.score])
    oh.close()
    ih.close()

mlstart = re.compile(r'(.*?)({{|\[\[)(.*)')
markupend = re.compile(r'(.*?)}}(.*)')
linkend = re.compile(r'(.*?)\]\](.*)')
def get_markup(instr):
  startm = mlstart.match(instr)
  if not startm:
    return None
  pre = startm.group(1)
  mtype = startm.group(2)
  rem = startm.group(3)

  if mtype == "{{":
    endm = markupend.match(rem)
  else:
    endm = linkend.match(rem)

  if not endm:
    # somehow found a markup tag with no end
    return None
  
  markup = endm.group(1)
  post = endm.group(2)

  return [pre, mtype, markup, post]

markuphandling = dict()
markuphandling['ANCHOR'] = 'drop'
markuphandling['CITATION'] = 'drop'
markuphandling['IPA'] = 'exclude'
markuphandling['INT:PROXYBLOCKREASON'] = 'drop'
markuphandling['FLAGICON'] = 'drop'
markuphandling['MATH'] = 'exclude'
markuphandling['MVAR'] = 'exclude'
markuphandling['REFLIST'] = 'exclude'
markuphandling['SFN'] = 'exclude'
markuphandling['仮リンク'] = 1
markuphandling['ヘルプページヘッダ'] = 'drop'
swmarkuphandling = dict()
swmarkuphandling['CITE'] = 'drop'
swmarkuphandling['DEFAULTSORT:'] = 'drop'
swmarkuphandling['デフォルトソート:'] = 'drop'
swmarkuphandling['FLAG'] = 'drop'
swmarkuphandling['INFOBOX'] = 'drop'
def handle_markup(mtype, markup, lh, link):
  if mtype == '[[':
    # link
    lsplit = markup.split('|')
    if len(lsplit) == 1:
      toret = lsplit[0]
      if toret.startswith('ファイル:'):
        return ''
      if toret.startswith('File:'):
        return ''
      return toret
    else:
      return lsplit[1]
    lh.write([markup, link, 0])

  if mtype == '{{':
    msplit = markup.split('|')
    tag = msplit[0].upper().rstrip()
    try:
      handling = markuphandling[tag]
    except KeyError:
      handling = 'special'
      for k, v in swmarkuphandling.items():
        if tag.startswith(k):
          handling = v
    
    if handling == 'exclude':
      return '%exclude%'
    if handling == 'drop':
      return ''
    if type(handling) is int:
      if handling < len(msplit):
        return msplit[handling]
      else:
        print("Out of bounds lookup for {}".format(markup))


    if len(msplit) == 2:
      if tag.startswith('LANG-'):
        if tag.endswith('-EN'):
          return msplit[1]
        else:
          return ''
    
    if len(msplit) > 2:
      langarg = msplit[1].upper()
      text = msplit[2]
      if tag == 'LANG' or tag == 'LANGWITHNAME':
        if langarg == 'EN':
          return text
        else:
          return '%exclude%'

  lh.write([markup, link, 0])
  return ''
    

rule wikipedia_nomarkup:
  input:
    "processed_data/wikipedia/nohtags.txt.gz"
  output:
    "processed_data/wikipedia/nomarkup.txt.gz"
  log:
    "makelog/markup.log.gz"
  run:
    limit = global_limit
    # Wiki-markup headings
    headre = re.compile('^={1,6}([^=]*)={1,6}$')
    # Wiki-markup bold/italics
    boldital = re.compile("'{2,5}([^']*)'{2,5}")
    # Input/output pipes
    ih = Input(input, limit)
    oh = Output(output[0])
    lh = Output(log[0])
    while ih.next():
      link = ih.note
      utext = ih.text
      utext = headre.sub(r'\1', utext)
      utext = boldital.sub(r'\1', utext)
      remtext = utext
      ret = get_markup(remtext)
      complete = ''
      while ret is not None:
        #pre, mtype, markup, post 
        newinner = handle_markup(ret[1], ret[2], lh, link)
        complete += ret[0]
        complete += newinner
        remtext = ret[3]
        ret = get_markup(remtext)
      complete += remtext

      #print("nomarkup converted {} to {} for {}".format(ih.text, complete, link))
      oh.write([complete, link, ih.score])

    oh.close()
    ih.close()

taghandling = dict()
taghandling["a"] = "innertext"
taghandling["abbr"] = "innertext"
taghandling["b"] = "innertext"
taghandling["big"] = "innertext"
taghandling["body"] = "innertext"
taghandling["blockquote"] = "innernl"
taghandling["br"] = "nl"
taghandling["br\\"] = "nl"
taghandling["br."] = "nl"
taghandling["br-"] = "nl"
taghandling["caption"] = "innertext"
taghandling["categorytree"] = "squash"
taghandling["ce"] = "exclude"
taghandling["center"] = "innernl"
taghandling["charinsert"] = "squash"
taghandling["chem"] = "exclude"
taghandling["cite"] = "innertext"
taghandling["code"] = "innertext"
taghandling["del"] = "squash"
taghandling["div"] = "innernl"
taghandling["dd"] = "innertext"
taghandling["dl"] = "innertext"
taghandling["dt"] = "innertext"
taghandling["em"] = "innertext"
taghandling["font"] = "innertext"
taghandling["gallery"] = "squash"
taghandling["hiero"] = "exclude"
taghandling["hr"] = "nl"
taghandling["html"] = "innertext"
taghandling["http:"] = "innertext"
taghandling["https:"] = "innertext"
taghandling["h1"] = "innernl"
taghandling["h2"] = "innernl"
taghandling["h3"] = "innernl"
taghandling["h4"] = "innernl"
taghandling["h5"] = "innernl"
taghandling["i"] = "innertext"
taghandling["imagemap"] = "squash"
taghandling["img"] = "innertext"
taghandling["includeonly"] = "innertext"
taghandling["ins"] = "innertext"
taghandling["inputbox"] = "squash"
taghandling["kbd"] = "innertext"
taghandling["li"] = "innernl"
taghandling["math"] = "exclude"
taghandling["noinclude"] = "squash"
taghandling["nowiki"] = "innertext"
taghandling["ol"] = "innertext"
taghandling["onlyinclude"] = "innertext"
taghandling["p"] = "innernl"
taghandling["philosophy"] = "notatag"
taghandling["poem"] = "innernl"
taghandling["pre"] = "innertext"
taghandling["q"] = "innertext"
taghandling["rb"] = "innertext"
taghandling["ref"] = "squash"
taghandling["references"] = "squash"
taghandling["remix"] = "notatag"
taghandling["rp"] = "squash"
taghandling["rt"] = "squash"
taghandling["ruby"] = "innertext"
taghandling["s"] = "innertext"
taghandling["section"] = "innertext"
taghandling["score"] = "exclude"
taghandling["script"] = "squash"
taghandling["small"] = "innertext"
taghandling["source"] = "squash"
taghandling["span"] = "innernl"
taghandling["strike"] = "innertext"
taghandling["strong"] = "innertext"
taghandling["sub"] = "exclude"
taghandling["sup"] = "exclude"
taghandling["syntaxhighlight"] = "squash"
taghandling["table"] = "innertext"
taghandling["td"] = "innernl"
taghandling["th"] = "innernl"
taghandling["timeline"] = "squash"
taghandling["title"] = "squash"
taghandling["tr"] = "innertext"
taghandling["tt"] = "innertext"
taghandling["u"] = "innertext"
taghandling["ul"] = "innertext"
taghandling["var"] = "innertext"

class HTMLStripper(html.parser.HTMLParser):
  def __init__(self, logger):
    super().__init__()
    self.text = ""
    self.sdepth = 0
    self.logger = logger
    self.note = ""

  def reset(self):
    super().reset()
    self.text = ""
    self.sdepth = 0

  def handle_starttag(self, tag, attrs):
    try:
      handling = taghandling[tag]
    except KeyError:
      if self.sdepth <= 0:
        #print("Unknown start tag: {}".format(tag))
        self.logger.write([tag, self.note, 0])
      handling = "notatag"

    if handling == "squash":
      self.sdepth += 1
      return

    if handling == "exclude":
      self.text += "%exclude%"
      self.sdepth += 1
      return

    if handling in ["innertext", "innernl"] :
      # ignored
      return

    if handling == "nl":
      self.text += "%lf%"
      return
    
    if handling == "notatag":
      if self.sdepth <= 0:
        self.text += "<{}>".format(tag)
      return

  def handle_endtag(self, tag):
    try:
      handling = taghandling[tag]
    except KeyError:
      if self.sdepth <= 0:
        self.logger.write([tag, self.note, 0])
      handling = "notatag"

    if handling == "squash":
      self.sdepth -= 1
      return
    
    if handling == "exclude":
      self.sdepth -= 1
      return

    if handling == "innertext":
      # ignored
      return
    
    if handling == "innernl":
      self.text += " %lf% "
      return

    if handling == "nl":
      # ignored
      return
    
    if handling == "notatag":
      if self.sdepth <= 0:
        self.text += "</{}>".format(tag)
      return

  def handle_data(self, data):
    if self.sdepth <= 0:
      self.text += data

rule wikipedia_notags:
  input:
    "processed_data/wikipedia/noxml.txt.gz"
  output:
    "processed_data/wikipedia/nohtags.txt.gz"
  log:
    "makelog/tags.log.gz"
  run:
    limit = global_limit
    # All full sentences (not phrases / clauses)
    # should contain one of the major particles
    sanitycheck = re.compile('[をがは]')
    ih = Input(input, limit)
    oh = Output(output[0])
    lh = Output(log[0])
    hs = HTMLStripper(lh)
    while ih.next():
      hs.note = ih.note
      hs.feed(ih.text)
      hs.close()
      usent = hs.text
      # the HTML parser base class interprets entities which could contain tab characters
      usent = usent.replace(sep, " ")
      hs.reset()

      if len(usent) > 5 and sanitycheck.search(usent):
        oh.write([usent, ih.note, ih.score])
    oh.close()
    ih.close()
    lh.close()

rule wikipedia_noxml:
  input:
    ls("raw_data/wikipedia/", "bz2")
  output:
    "processed_data/wikipedia/noxml.txt.gz"
  run:
    limit = global_limit
    title = ""
    titlecount = 0
    sanitycheck = re.compile('[をがは]')
    oh = Output(output[0])
    for infn in input:
      with bz2.open(infn, mode="rt") as inf:
        # Need to use the iterative mode of etree
        # The XML is so big it will exhaust all available
        # memory unless your PC is huge
        titleskip = False
        for event, elem in xml.etree.ElementTree.iterparse(inf):
          limit -= 1
          if limit <= 0 and global_limit != 0:
            break
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
            if title.startswith("Template:"):
              titleskip = True
            if title.startswith("Wikipedia:削除依頼"):
              titleskip = True
            if title.startswith("Template‐ノート:"):
              titleskip = True
            if title.endswith(".js"):
              titleskip = True
          if not titleskip and elem.tag.endswith("}text") and elem.text is not None:
            article = elem.text.replace("\n", " %lf% ")
            article = article.replace(sep, " ")
            oh.write([article, title, 0])
          elem.clear()
    print("wikipedia_noxml closing oh")
    oh.close()

rule tatoeba_idreplace:
  # tatoeba-to is <text> \t <space separated list of ids>
  # sentences is <id> \t <lang> \t <text>
  input:
    "processed_data/tatoeba_link/tatoeba-to.txt.gz",
    "processed_data/tatoeba_extract/sentences.csv.bz2"
  output:
    "processed_data/output/tatoeba.txt.gz"
  run:
    limit = global_limit
    with gzip.open(output[0], mode="wt") as of:
      with bz2.open(input[1], mode="rt") as senf:
        senhash = dict()
        interestingids = set()
        engidhash = dict()
        print("Generating input dictionary")
        with gzip.open(input[0], mode="rt") as inf:
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
    "processed_data/tatoeba_link/tatoeba-from.txt.gz",
    "processed_data/tatoeba_extract/links.csv.bz2"
  # output format should be:
  # <text> \t <space separated list of other sentence ids>
  output:
    "processed_data/tatoeba_link/tatoeba-to.txt.gz"
  run:
    with gzip.open(output[0], mode="wt") as of:
      links = dict()
      with gzip.open(input[0], mode="rt") as inf:
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
      with gzip.open(input[0], mode="rt") as inf:
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
    "processed_data/tatoeba_link/tatoeba-from.txt.gz"
  run:
    limit = global_limit
    with gzip.open(output[0], mode="wt") as of:
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
    "raw_data/frequency_analysis/charfreq.txt",
    "raw_data/frequency_analysis/symbols.txt"
  output:
    "processed_data/frequency_analysis/charfreq.pkl"
  run:
    with open(output[0], mode="wb") as of:
      with open(input[0], mode="rt") as inf:
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

          # upper case half width
          hkey = jaconv.z2h(k, kana=True, digit=True, ascii=True)
          alternatekey = hkey.upper()
          if alternatekey != hkey:
            print("(hw upper) {} = {}".format(k, alternatekey))
            if alternatekey in freq:
              print("Warning: (upper) Multiple code points simplify to {}".format(k))
            else:
              freq[alternatekey] = v

          # upper case full width
          alternatekey = k.upper()
          if alternatekey != k:
            print("(fw upper) {} = {}".format(k, alternatekey))
            if alternatekey in freq:
              print("Warning: (upper) Multiple code points simplify to {}".format(k))
            else:
              freq[alternatekey] = v
        
        with open(input[1], mode="rt") as symf:
          for line in symf:
            c = line[0]
            print("(symbol) {}".format(c))
            if c not in freq:
              freq[c] = ["Symbol", None]

        pickle.dump(freq, of)
