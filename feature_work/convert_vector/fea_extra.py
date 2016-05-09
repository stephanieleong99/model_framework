# encoding:utf8
from __future__ import unicode_literals
import codecs
import os
from predifined_methods import Feature
from utils.CONST import DATA_DIR as root
import utils.CONST as cst
from utils.CONST import LABEL
import multiprocessing as mp
import optparse
import time


def init_arguments():
    parser = optparse.OptionParser()
    parser.add_option('-m', type=str, dest='mode',
                      help="work mode")
    return parser.parse_args()


def normal(fc, fea_value, fea):
    return FEA.__getattribute__(cst.parse_method(fc.method)[0])(fc.name, fea_value)


def none(fc, fea_value, fea):
    return


def pair(fc, fea_value, fea):
    fea_name_list = fc.name.split("&")
    fea_value_list = [fea[num - 1] for num in [FEA.name_id_dict[fea_name] for fea_name in fea_name_list]]
    # print fea_name_list,fea_value_list,FEA.__getattribute__(fc.method)(fea_name_list, fea_value_list)
    return FEA.__getattribute__(fc.method)(fea_name_list, fea_value_list)


def one_line(line):
    with cst.TimeRecord("initial") as _:
        fea = map(lambda x: x.strip(), line.split("\t"))[:len(FEA.name_id_dict)]
        # 获取该样本的label
        one_lable = str(int(fea[FEA.name_id_dict[LABEL] - 1]))

    def one_fea((n, fea_value)):
        """
        1. n,fea_value
        2. fea_name or fea_name_list ,fea_value or fea_value_list

        调用获取v
        :return:
        """

        fea_name = FEA.id_name_dict[n]
        fc = FEA.name_conf_dict[fea_name]
        fun_key = {"cate": normal, "origin": normal, "number": normal, "none": none, "pair": pair}
        if fc.name != cst.LABEL:
            return fun_key[fc.method.split("#")[0]](fc, fea_value, fea)

    rs = filter(lambda x: x, map(one_fea, enumerate(fea + [0] * (max_len - len(fea)), start=1)))
    if rs and max_feature_id_num == rs[-1][0]:
        data_line = " ".join(map(lambda x: ":".join(map(str, x)), sorted(rs, key=lambda x: int(x[0]))))
    else:
        data_line = " ".join(map(lambda x: ":".join(map(str, x)), sorted(rs, key=lambda x: int(x[0])))
                             + [max_feature_id_num + ":0"])
    return '\t'.join([one_lable, data_line])


if __name__ == '__main__':
    FEA = Feature()
    # /Users/lt/PycharmProjects/model_framework/feature_work/config/user_features/feas_reconstruct
    FEA.config = os.path.join(cst.APP_PATH, 'feas_reconstruct')
    FEA.read_conf()
    max_len = len(FEA.name_id_dict)

    args = init_arguments()[0]
    # cst.app_file = user_feature_raw_dup
    file = cst.RAW_FILE_NAME if args.mode == "train" else cst.TEST_FILE_NAME
    data_file = os.path.join(root, file)
    # /Users/lt/data/user_features_v_1_test_features_lines
    feature_lines = os.path.join(root, "_".join([cst.APP, "features_lines"]))

    with codecs.open(data_file, "r", "utf8") as f:
        data = f.readlines()[1:]

    max_feature_id_num = str(sorted(FEA.feature_dict.values(), key=lambda x: int(x))[-1])
    print "max_feature_id_num", max_feature_id_num

    t = time.time()
    with cst.TimeRecord("total") as _:
        pool = mp.Pool(32)
        rs = pool.map(one_line, data)
        # rs = map(one_line, data)
        with codecs.open(feature_lines, 'w', 'utf8') as f:
            f.write('\n'.join(rs))

    print "eclapse {0}s".format(time.time() - t)