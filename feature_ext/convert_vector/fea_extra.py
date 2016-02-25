# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import root

FEA = Feature()
FEA.config = '/Users/dongjian/PycharmProjects/UserDetected/feature_ext/convert_vector/test/feas_reconstruct'
FEA.read_conf()

data_file = os.path.join(root, "fea_raw_file_tmp")
feas = os.path.join(root, "features_lines")
with codecs.open(data_file, "r", "utf8") as f:
    ins = f.readlines()
    data = ins[1:]

label_name = "punish_status"


def print_fea_vector(data, split_fea=True):
    with codecs.open(feas, "w", "utf8") as file:
        for d in data:
            fea = d.split("\t")
            try:
                one_lable = str(int(float(fea[FEA.fea_number_dict[label_name] - 1])))
            except:
                one_lable = "0"

            fea_list = []


            for n, f in enumerate(fea):
                n += 1
                if n != FEA.fea_number_dict[label_name]:
                    fea_name = FEA.num_fea_dict[n]
                    v = FEA.__getattribute__(FEA.fea_conf[fea_name].method)(fea_name, f)
                    fea_list.append([str(v), "1"])

        one_data = " ".join([":".join(fe) for fe in fea_list])
        file.write("\t".join([one_lable, one_data, "\n"]))


print_fea_vector(data, split_fea=True)
FEA.print_feas()
