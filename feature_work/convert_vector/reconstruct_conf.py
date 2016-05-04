# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import data_dir as root
import utils.CONST as cst
import re

FEA = Feature()
FEA.read_conf()

# /Users/lt/data/user_feature_raw_dup
data_file = os.path.join(root, cst.app_file)
# /Users/lt/data/user_feature_origin_v_1_test_origin_features_lines
feas = os.path.join(root, "_".join([cst.app, "features_lines"]))
# /Users/lt/PycharmProjects/model_framework/feature_work/config/user_feature_origin/feas_reconstruct
rs_file = os.path.join(cst.app_root_path, 'feas_reconstruct')

# print rs_file
open(rs_file, 'w').close()

with codecs.open(data_file, "r", "utf8") as f:
    ins = f.readlines()
    data = ins[1:]

label_name = "punish_status"

c_args = {",": "0x32"}


def print_fea_vector(data, split_fea=True):
    with codecs.open(feas, "w", "utf8") as file:
        for d in data[:]:
            fea = d.split("\t")
            for n, f in enumerate(fea):
                n += 1
                # print FEA.fea_number_dict
                if n != FEA.fea_number_dict[label_name] and n <= len(FEA.fea_number_dict):
                    fea_name = FEA.num_fea_dict[n]
                    FEA.fea_number_value_list[fea_name].append(f)


# print FEA.num_fea_dict


def generate_conf(fea_number_value_list):
    for num, name in FEA.num_fea_dict.items():
        k, v = name, FEA.fea_number_value_list[name]
        conf = FEA.fea_conf[k]
        yield conf




def remove_dup(value_list):
    def wash_cate(value):
        if value == "NULL":
            return "0"
        else:
            return value

    value_list = map(wash_cate, value_list)
    return list(set(value_list))


def process_cate(conf, value,frac):
    filter_sets = set(conf.filter_threds) if conf.filter_threds else []
    return filter(lambda x: x not in filter_sets, remove_dup(FEA.fea_number_value_list[conf.name]))

def wash_data(data):
        try:
            data = float(data)
            return True if 30000000 > data >= 0 else False
        except:
            return False

def process_number(conf, value,frac):


    v = FEA.fea_number_value_list[conf.name]

    after_sorted = filter(lambda x: wash_data(x),
                          sorted([x.strip() for x in v if cst.isfloat(x)], key=lambda x: float(x)))
    after_filtered = filter(lambda x: not float(conf.filter_threds[0]) <= float(x) <= float(conf.filter_threds[1]),
                            after_sorted) if conf.filter_threds else after_sorted
    sorted_v = after_filtered

    def equal_freq(v,frac):
        # print v,'v'
        freq = sorted(list(set([v[index] for index in range(1, len(v), len(v) / frac)] + [v[-1]])),
                      key=lambda x: float(x))
        return freq

    def equal_dis(v,frac):
        l, h = float(v[0]), float(v[-1])
        l = l if l >= 0 else 0
        KEY = frac
        return sorted(remove_dup([str(l + i * (h - l) / KEY) for i in range(int(KEY) + 1)]), key=lambda x: float(x))

    data_huafen = locals().get("equal_{key}".format(**{"key": value}), None)
    if not data_huafen:
        data_huafen = equal_freq
        # print data_huafen(sorted_v)
    return sorted(list(set(map(lambda x: str(float('%0.3f' % float(x))), data_huafen(sorted_v,frac)))),key=lambda x:float(x))

def process_origin(conf,value,frac):
    v = FEA.fea_number_value_list[conf.name]
    after_sorted = filter(lambda x: wash_data(x),
                           sorted([x.strip() for x in v if cst.isfloat(x)], key=lambda x: float(x)))
    after_filtered = filter(lambda x: not float(conf.filter_threds[0]) <= float(x) <= float(conf.filter_threds[1]),
                            after_sorted) if conf.filter_threds else after_sorted
    return map(lambda x:str(float('%0.3f' % float(x))),[after_filtered[0],after_filtered[-1]])

def process_pair(conf, value,frac):
    return []


def one_conf(conf):
    def print_conf(conf, values):
        with codecs.open(rs_file, 'a', 'utf8') as f:
            f.write(
                    ','.join(map(str, [conf.name, conf.method, "#".join(conf.filter_threds) if conf.filter_threds else "",conf.status,
                                       '#'.join([v.strip() for v in values]).replace(",", "0x32")])) + '\n')
            print ','.join(map(str, [conf.name, conf.method, conf.status,"#".join(conf.filter_threds) if conf.filter_threds else "",
                                     '#'.join([v.strip() for v in values]).replace(",", "0x32")]))

    if conf.method == "none" or conf.name == cst.label_name:
        print_conf(conf, [])
        return
    key, value,frac = cst.parse_method(conf.method)
    data_method = globals().get("process_{key}".format(**{"key": key}))
    rs = data_method(conf, value,frac)
    print_conf(conf, rs)


def process(fea_number_value_list):
    print_fea_vector(data)
    confs = [conf for conf in generate_conf(fea_number_value_list)]
    print len(confs)
    map(one_conf, confs)


if __name__ == "__main__":
    process(FEA.fea_number_value_list)
    # print mul_replace(",111,21123")
