# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import root

FEA = Feature()
FEA.read_conf()

data_file = os.path.join(root, "fea_raw_file_tmp")
feas = os.path.join(root, "features_lines")
rs = '/Users/dongjian/PycharmProjects/UserDetected/feature_ext/convert_vector/test/feas_reconstruct'

with codecs.open(data_file, "r", "utf8") as f:
    ins = f.readlines()
    data = ins[1:]

label_name = "punish_status"


def print_fea_vector(data, split_fea=True):
    with codecs.open(feas, "w", "utf8") as file:
        for d in data:
            fea = d.split("\t")
            for n, f in enumerate(fea):
                n += 1
                if n != FEA.fea_number_dict[label_name]:
                    fea_name = FEA.num_fea_dict[n]
                    FEA.fea_number_value_list[fea_name].append(f)


print FEA.num_fea_dict


def print_conf_by_space():
    with codecs.open(rs, 'wb', 'utf8') as f:
        for num, name in FEA.num_fea_dict.items():
            k, v = name, FEA.fea_number_value_list[name]
            conf = FEA.fea_conf[k]
            if conf.method == "number":
                v = sorted([x for x in v if x != "NULL"], key=lambda x: float(x))
                l, h = float(v[0]), float(v[-1])
                l = l if l >= 0 else 0
                f.write(','.join([conf.name, conf.method,
                                  '#'.join([str(l + i * (h - l) / 10.0) for i in range(10 + 1)])]) + '\n')
            else:
                f.write(','.join([conf.name, conf.method, '']) + '\n')


def print_conf_by_freq():
    with codecs.open(rs, 'wb', 'utf8') as f:
        for num, name in FEA.num_fea_dict.items():
            k, v = name, FEA.fea_number_value_list[name]
            conf = FEA.fea_conf[k]
            if conf.method == "number":
                # if conf.name =="one2n_phone_num":
                v = filter(lambda x: float(x) >= 0, sorted([x for x in v if x != "NULL"], key=lambda x: float(x)))
                freq = sorted(list(set([v[index] for index in range(1, len(v), len(v) / 10)] if len(v) > 10 else v)),
                              key = lambda x: float(x))

                # print freq
                f.write(','.join([conf.name, conf.method, '#'.join(freq)]) + '\n')
            else:
                f.write(','.join([conf.name, conf.method, '']) + '\n')


print_fea_vector(data, split_fea=False)
print_conf_by_freq()
# FEA.print_feas()
