# encoding:utf8
import os
import codecs
import json
import itertools
from utils.CONST import data_dir as root
import utils.CONST as cst

fea_name = "_".join([cst.app,"features_coef"])
fea_coef = os.path.join(root, fea_name)
features_ids = os.path.join(root, "_".join([cst.app,"features_ids"]))
features_names = os.path.join(root, fea_name + "_with_names")


with codecs.open(features_ids, 'r', 'utf8') as f:
    id_fea = {x.split("&#&")[1].strip(): x.split("&#&")[0] for x in f.readlines()}

with codecs.open(fea_coef, 'r', 'utf8') as f:
    rs = sorted([(id_fea[x.split("\t")[0].strip()], x.split("\t")[1].strip()) for x in f.readlines()],
                key=lambda x: x[1], reverse=True)
    print json.dumps(rs, indent=4)
    with codecs.open(features_names, 'w', 'utf8') as f:
        f.write("\n".join([":".join(x) for x in sorted(rs,key=lambda x:-float(x[1]))]))



