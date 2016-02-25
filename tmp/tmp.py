import  codecs
data = "/Users/dongjian/data/tmp"
with codecs.open(data,'r','utf8') as f:
    bucket = f.readlines()

for b in bucket:
    print "select "+b.replace(",","\nunion all").strip()
