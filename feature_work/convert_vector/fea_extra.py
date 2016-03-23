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
    data = f.readlines()[1:]

label_name = "punish_status"



def normal(fc, fea_value, fea):
    return FEA.__getattribute__(cst.parse_method(fc.method)[0])(fc.name, fea_value)


def none(): return


def pair(fc, fea_value, fea):
    fea_name_list = fc.name.split("&")
    fea_value_list = [fea[num - 1] for num in [FEA.fea_number_dict[fea_name] for fea_name in fea_name_list]]
    return FEA.__getattribute__(fc.method)(fea_name_list, fea_value_list)


def one_line(line):
    fea = line.split("\t")
    one_lable = str(int(fea[FEA.fea_number_dict[label_name] - 1]))

    def one_fea((n, fea_value)):
        '''
        1. n,fea_value
        2. fea_name or fea_name_list ,fea_value or fea_value_list

        调用获取v
        :return:
        '''

        fea_name = FEA.num_fea_dict[n]
        fc = FEA.fea_conf[fea_name]
        fun_key = {"cate": normal, "number": normal, "none": none, "pair": pair}
        return fun_key[fc.method](fc, fea_value, fea)
    rs = map(one_fea, enumerate(fea, start=1))
    data_line = " ".join(map(lambda x:":".join([x,"1"]),sorted(rs, key=lambda x: int(x[0]))))
    return '\t'.join([one_lable,data_line])



print_fea_vector(data, split_fea=True)
