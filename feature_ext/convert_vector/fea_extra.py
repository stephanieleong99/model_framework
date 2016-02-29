# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import data_dir as root
import utils.CONST as cst


FEA = Feature()

FEA.config = os.path.join(cst.app_root_path, 'feas_reconstruct')
FEA.read_conf()

data_file = os.path.join(root, cst.app_file)
feature_lines = os.path.join(root, "_".join([cst.app, "features_lines"]))
with codecs.open(data_file, "r", "utf8") as f:
    ins = f.readlines()
    data = ins[1:]

label_name = "punish_status"


def print_fea_vector(data, split_fea=True):
    with codecs.open(feature_lines, "w", "utf8") as file:
        for d in data:
            fea = d.split("\t")
            try:
                one_lable = str(int(float(fea[FEA.fea_number_dict[label_name] - 1])))
            except:
                one_lable = "0"

            fea_list = []

            for n, fea_value in enumerate(fea):
                n += 1
                if n != FEA.fea_number_dict[label_name]:
                    fea_name = FEA.num_fea_dict[n]
                    v = FEA.__getattribute__(cst.parse_method(FEA.fea_conf[fea_name].method)[0])(fea_name, fea_value)
                    fea_list.append([str(v), "1"])

            # print FEA.num_fea_dict
            if len(FEA.num_fea_dict) > len(fea):  # 有交叉特征
                print "???"
                n += 1
                pair_name = FEA.num_fea_dict[n]
                fea_name_list = FEA.num_fea_dict[n].split("&")
                fea_value_list = [fea[num - 1] for num in [FEA.fea_number_dict[fea_name] for fea_name in fea_name_list]]
                v = FEA.__getattribute__(FEA.fea_conf[pair_name].method)(fea_name_list, fea_value_list)

            one_data = " ".join([":".join(fe) for fe in fea_list])
            file.write("\t".join([one_lable, one_data, "\n"]))


print_fea_vector(data, split_fea=True)
FEA.print_feas()
