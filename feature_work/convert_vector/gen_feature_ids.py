# encoding:utf8
import os
import codecs

from utils.CONST import data_dir as root
import json
import utils.CONST as cst
import arrow
import itertools
from feature_work.convert_vector.predifined_methods import Conf
from utils.CONST import label_name

FEATURE_IDS = "feature_ids"

GOLD_SPLIT = "__"
FEA_ID_SPLIT = "&#&"

feas_path = os.path.join(cst.app_root_path, 'feas')
features_ids_path =  os.path.join(root, "_".join([cst.app, "features_ids"]))
feas_reconstruct_path = os.path.join(cst.app_root_path, 'feas_reconstruct')
feature_id = 1
conf_dict = {}


def one_conf(line):
    global conf_dict, feature_id
    conf = Conf(*line.split(","))
    if conf.method == "none" or conf.name == label_name:
        return
    conf_dict[conf.name] = conf
    conf.parse_ars(conf_dict)
    # print conf.feature_list
    feature_id += len(conf.feature_list)

    # print ["__".join([conf.name, fea]) + FEA_ID_SPLIT + str(fea_id) for fea in conf.feature_list
    #         for fea_id in xrange(feature_id - len(conf.feature_list),feature_id,1)]
    if conf.status:
        return ["__".join([conf.name, fea]) + FEA_ID_SPLIT + str(fea_id) for fea,fea_id in zip(conf.feature_list,xrange(feature_id - len(conf.feature_list),feature_id,1))]


with codecs.open(feas_reconstruct_path, 'r', 'utf8') as f, codecs.open(features_ids_path, 'wb', 'utf8') as f_w:

    feature_id_list = list(itertools.chain(*filter(lambda x: x, map(one_conf, f.readlines()))))
    print "\n".join(feature_id_list)
    f_w.write("\n".join(feature_id_list))
