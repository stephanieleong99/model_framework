import codecs
import os
import math
from utils.CONST import DATA_DIR as root
import utils.CONST as cst
import multiprocessing as mp

feature_list = os.path.join(root, "_".join([cst.APP, "features_ids"]))
data_file = os.path.join(root, "_".join([cst.APP, "features_lines"]))
result_path = os.path.join(root, "_".join([cst.APP, "features_coef"]))


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


def correlation_coefficient(x_list, y_list):
    if len(x_list) == 0 or len(y_list) == 0:
        print "length of x_list is", len(x_list), "#  length of y_list is", len(y_list)
    x_sum = sum(x_list)
    y_sum = sum(y_list)
    n = len(x_list)
    range_n = range(n)
    # print x_sum,y_sum,n,range_n
    xy_sum = sum([x_list[i] * y_list[i] for i in range_n])
    xx_sum = sum([x_list[i] * x_list[i] for i in range_n])
    yy_sum = sum([y_list[i] * y_list[i] for i in range_n])
    fenzi = float(n) * xy_sum - x_sum * y_sum
    fenmu = math.sqrt(n * xx_sum - x_sum * x_sum) * math.sqrt(n * yy_sum - y_sum * y_sum)
    # print fenzi,fenmu
    return fenzi / fenmu if fenmu else 0.0


def one_feature_co(p):
    label = p[0]
    data = p[1]
    feature = str(p[2])
    return (feature
            , correlation_coefficient(
            label,
            [1 if feature in d[1] else 0 for d in data]))


features = load_feature()
print 'feature size =', len(features)

data = load_data()

print 'data size =', len(data)
# for d in data:
#     print d[1]
label = [1 if int(d[0]) == 1 else 0 for d in data]
print 'label size =', len(label), ' label sum =', sum(label)

param = [(label, data, f) for f in features]

pool = mp.Pool(16)
rst = pool.map(one_feature_co, param)

count = 0
result_lines = []
for r in rst:
    count += 1
    if count % 1000 == 0:
        print 'parsed %d features' % count
        print r[0], r[1]
    result_lines.append(u'%s\t%f' % (r[0], r[1]))

with codecs.open(result_path, 'w', 'utf8') as f:
    f.write(u'\n'.join(result_lines))
