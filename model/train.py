from __future__ import unicode_literals
from sklearn import cross_validation
import sklearn
import codecs
import utils.CONST as const
from sklearn import cross_validation
from sklearn.naive_bayes import GaussianNB,MultinomialNB,BernoulliNB
from sklearn import metrics
import numpy as np
import random
import itertools
from datetime import timedelta, date

with codecs.open(const.data_file, "rb", "utf8") as f:
    raw = f.readlines()[1:-1]
    get_y = lambda x: int(x.split("\t")[1].split(",")[0])
    get_x = lambda x: [int(x) for x in x.split("\t")[1].split(",")[1].split(" ")]
    # Y = [ 0 if x ==1 else 1 for x in [get_y(x) for x in raw]]
    Y = [get_y(x) for x in raw]
    X = [get_x(x) for x in raw]

thred = 1 * len(X) / 2
train_x = np.array(X[:thred])
train_y = np.array(Y[:thred])
test_x = np.array(X[thred:])
test_y = np.array(Y[thred:])


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


# def parse():
def cross_split(X, Y, n):
    trunk = len(X) / n
    split_x = [x for x in chunks(X, trunk)][:n]
    split_y = [x for x in chunks(Y, trunk)][:n]
    print [len(x) for x in split_x]
    for x in xrange(n):
        choose = random.randint(0, n - 1)
        trains = [x for x in range(n) if x != choose]
        train_x = list(itertools.chain(*[split_x[x] for x in trains]))
        train_y = list(itertools.chain(*[split_y[x] for x in trains]))
        test_x = split_x[choose]
        test_y = split_y[choose]
        yield train_x, train_y, test_x, test_y


def predict_mec(train_x, train_y, test_x, test_y):
    clf = MultinomialNB(alpha=1,class_prior=[ 6 , 1])#
    clf.fit(train_x, train_y)
    pred_y = clf.predict(test_x)
    return evaluate(test_y, pred_y)


def evaluate(actual, pred):
    m_precision = sum([x[0] == x[1] for x in zip(actual, pred)]) / float(len(actual))
    m_recall = sum([x[0] == x[1] and x[1] == 1 for x in zip(actual, pred)]) / float(sum([x == 1 for x in actual]))
    TP = sum([x[0] == x[1] and x[0] == 1 for x in zip(actual, pred)])
    TN = sum([x[0] == x[1] and x[0] == 0 for x in zip(actual, pred)])
    FP = sum([x[0] != x[1] and x[0] == 0 for x in zip(actual, pred)])
    FN = sum([x[0] != x[1] and x[0] == 1 for x in zip(actual, pred)])
    print TP, TN, FP, FN
    print sum(actual), len(actual) - sum(actual), sum(pred), len(actual)
    print sum([x[0] == x[1] and x[1] == 1 for x in zip(actual, pred)])
    print float(len([x == 1 for x in actual]))
    # print float()
    print  'precision:{0:.3f}'.format(m_precision)
    print 'pos precision:{0:.3f}'.format(TP/float(TP+ FP))
    print 'neg precision:{0:.3f}'.format(TN/float(TN+ FN))

    print 'recall:{0:0.3f}'.format(m_recall)
    print 'pos recall:{0:0.3f}'.format(TP/float(TP+FN))
    print 'neg recall:{0:0.3f}'.format(TN/float(TN+FP))

    print 'fp/tn + fp ', FP / float(TN + FP)

    print 'TP/float(TP+ FN) ', TP / float(TP + FN)
    print 'TP/float(TP+ FP)', TP/float(TP+ FP)

    return m_precision, m_recall, TP / float(TP + FN), FP / float(TN + FP),TP/float(TP+ FP)


rs = []
for train_x, train_y, test_x, test_y in cross_split(X, Y, 5):
    rs.append(predict_mec(train_x, train_y, test_x, test_y))

print "precision:", sum([x[0] for x in rs]) / len(rs), \
    "recall:", sum([x[1] for x in rs]) / len(rs), \
    "TP/float(TP+ FN)", sum(
        [x[2] for x in rs]) / len(rs), \
    "fp/tn+fp", sum(
        [x[3] for x in rs]) / len(rs),\
    "TP/float(TP+ FP)",\
    sum([x[4] for x in rs]) / len(rs)
    # print len(train_x), len(train_y), len(test_x), len(test_y)
# clf = GaussianNB()
# clf.fit(train_x, train_y)
# pred_y = clf.predict(test_x)
# evaluate(test_y,pred_y)




#
# scores = cross_validation.cross_val_score(clf, X, Y, cv=10)
# print scores
# print("Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2))

# for i in x
