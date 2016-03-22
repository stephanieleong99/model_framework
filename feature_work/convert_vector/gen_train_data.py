# encoding:utf8
import codecs
import os
import math
from utils.CONST import data_dir as root
import utils.CONST as cst
import multiprocessing as mp
import re

feature_ids_path = os.path.join(root, "_".join([cst.app, "features_ids"]))
feature_lines_path = os.path.join(root, "_".join([cst.app, "features_lines"]))
coef_path = os.path.join(root, "_".join([cst.app, "features_coef"]))

choosed_feature_lines = os.path.join(root, "_".join([cst.app, "choosed_feature_lines"]))
choosed_features = os.path.join(root, "_".join([cst.app, "choosed_feature"]))

# 读取所有特征
with codecs.open(feature_lines_path, 'r', 'utf8') as f:
    def one_fea(fea):
        fsp = fea.split('\t')
        label, features = fsp[0], [key.split(":")[0] for key in fsp[1].split(" ")]
        return label, features


    feature_lines = map(one_fea, f.readlines())
# 读取字典
with codecs.open(feature_ids_path, 'r', 'utf8') as f:
    feature_to_num = dict([map(lambda x:x.strip(),l.split("&#&")) for l in f.readlines()])
    num_to_feature = {v: k for k, v in feature_to_num.items()}

# 重新生成feature_lines
with codecs.open(coef_path, 'r', 'utf8') as f:
    coef = sorted([x.split("\t") for x in f.readlines()], key=lambda x: abs(float(x[1])))

top = 1500
choosed_features_ids = set([f for f, v in coef[:top]])
print choosed_features_ids
with codecs.open(choosed_feature_lines, 'w', 'utf8') as f_1, codecs.open(choosed_features, 'w', 'utf8') as f_2:
    def one_line((l, fs)):
        return '\t'.join([l, ' '.join([':'.join(map(str,(f, 1))) for f in filter(lambda x: x in choosed_features_ids, fs)])])

    print num_to_feature
    f_2.write('\n'.join(['&#&'.join([num_to_feature[f], f]) for f in choosed_features_ids]))
    f_1.write('\n'.join(map(one_line, feature_lines)))
