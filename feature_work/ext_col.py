# encoding:utf8
import codecs

with codecs.open("tmp", 'r', 'utf8') as f:
    # def ext(st):
    #     all = st.split()
    #     print all
    #     sur_max = lambda x: "max(" + str(x) + ") " + str(x) + ","
    #     return [ a.split(".")[1] if len(a) > 0 else None for a in all]
    #
    # rs = filter(lambda x: x, map(ext, f.readlines()))
    # print '\n'.join(rs)
    l = f.readline()
    print ',\n'.join([",".join([x,"cate"]) for x in l.split("\t")])