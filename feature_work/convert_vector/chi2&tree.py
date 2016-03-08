from sklearn.datasets import load_iris
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.ensemble import ExtraTreesClassifier

import codecs
import os
import math
from utils.CONST import root
import multiprocessing as mp


d = {}
d.clear()

iris = load_iris()
X, y = iris.data, iris.target
clf = ExtraTreesClassifier()

feature_list = os.path.join(root, "features_ids")
data_file = os.path.join(root, "features_lines")
result_path_chi2 = os.path.join(root, "features_chi2")
result_path_tree = os.path.join(root, "features_tree")


def load_feature():
    with codecs.open(feature_list, 'r', 'utf8') as f:
        return [int(line.split("&#&")[1].strip()) for line in f.readlines()[:]]


def constru_set(line):
    s = line.split()
    if len(s) > 1:
        d = {}
        for one in s[1:]:
            term = one.split(':')
            if term[0] and term[1]:
                d[term[0]] = int(term[1])
        return (s[0], d)


def load_data():
    pool = mp.Pool(10)
    with codecs.open(data_file, 'r', 'utf8') as f:
        lines = f.readlines()
        return pool.map(constru_set, lines)


def parse_to_dense(data):
    tmp = []
    y = data[0]
    x = set([int(x) for x in data[1].keys()])  # set

    def is_in_fea(f):
        return 1 if f in x else 0

    return map(is_in_fea, fea)


fea = load_feature()

data = load_data()

pool = mp.Pool(10)
X = pool.map(parse_to_dense, data)  # get x
y = [d[0] for d in data]

rs_chi2 = chi2(X, y)
clf.fit(X, y).transform(X)
rs_tree = clf.feature_importances_

for n, x in enumerate(list(rs_chi2[0])):
    print n,float(x)

print rs_tree
with codecs.open(result_path_chi2, 'wb', 'utf8') as f_chi2, codecs.open(result_path_tree, 'wb', 'utf8') as f_tree:
    def write_file(f, data):
        f.write('\n'.join(['\t'.join((str(n + 1), str(format(float(x),'0.10f')))) for n, x in enumerate(list(data))]))


    write_file(f_chi2, rs_chi2[0])
    write_file(f_tree, rs_tree)
