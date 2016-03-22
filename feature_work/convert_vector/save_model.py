# encoding:utf8
import codecs
import os
import math
from utils.CONST import data_dir as root
import utils.CONST as cst
import multiprocessing as mp
import re
import random
from model.liblinear.python.liblinearutil import *

feature_ids_path = os.path.join(root, "_".join([cst.app, "features_ids"]))
feature_lines_path = os.path.join(root, "_".join([cst.app, "features_lines"]))
coef_path = os.path.join(root, "_".join([cst.app, "features_coef"]))

choosed_feature_lines = os.path.join(root, "_".join([cst.app, "features_lines"]))
choosed_features = os.path.join(root, "_".join([cst.app, "choosed_feature"]))

model_path = os.path.join(root, "_".join([cst.app, "_model"]))
# 读取下feature_lines
with codecs.open(choosed_features, 'r', 'utf8') as f:
    feature_to_num = dict([map(lambda x: x.strip(), l.split("&#&")) for l in f.readlines()])
    num_to_feature = {v: k for k, v in feature_to_num.items()}

# 读取features lines
with codecs.open(choosed_feature_lines, 'r', 'utf8') as f:
    def one_line(line):
        lsp = map(lambda x: x.strip(), line.split("\t"))
        # return int(lsp[0]), dict([map(int,l.split(":")) for l in lsp[1].split(" ") if l[0] in num_to_feature])
        return int(lsp[0]), dict([map(int,l.split(":")) for l in lsp[1].split(" ")])


    data = f.readlines()[:]
    random.shuffle(data)
    format = map(one_line, data)
    y = [d[0] for d in format]
    x = [d[1] for x in format]

print "train data size : {0}".format(len(data))
print "x size is {0},y size is {1}".format(len(x), len(y))

# print sum(y)
pos_num = len(filter(lambda x: x == 1, y))
neg_num = len(filter(lambda x: x == 0, y))

ratio = pos_num / neg_num
print pos_num,neg_num,ratio
thred = (len(y) / 4) * 3
para = "-c 0.5 "
model = train(y[:thred], x[:thred], para)
# whole_model = train(y[:], x[:], para)
save_model(model_path, model)

# 评价
p_label, p_acc, p_val = predict(y[thred:], x[thred:], model)
value = zip(y[thred:], p_label)

pos_len = len([x for x in y[thred:] if x == 1])
neg_len = len([x for x in y[thred:] if x == 0])
print "pos:{0},neg:{1}".format(pos_len, neg_len)
TP = sum(map(lambda x: x[0] == 1 and x[1] == 1, value))
TN = sum(map(lambda x: x[0] == 0 and x[1] == 0, value))
FP = sum(map(lambda x: x[0] == 0 and x[1] == 1, value))
FN = sum(map(lambda x: x[0] == 1 and x[1] == 0, value))

print "TP:{0},TN:{1},FP:{2},FN:{3}".format(TP,TN,FP,FN)
print "假证率:{1}".format(FP / (TP + FP))  # 假正率, fp/所有正
print "ACC:{1} MSE:{2} SCC:{3}".format(*evaluations(y[thred:], p_label))
