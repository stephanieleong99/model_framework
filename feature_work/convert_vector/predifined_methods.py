# encoding:utf8
import os
import codecs

from utils.CONST import data_dir as root
import json
import utils.CONST as cst

config = os.path.join(cst.app_root_path, 'feas')


def gen_cates(method, arrs):
    def cate(arrs):
        return {ar: 1 for ar in arrs.split("#")}

    def number(arrs):
        if len(arrs) <= 1: return []
        arrs_list = [float(x) for x in arrs.split("#")]
        return arrs_list

    def pair(arrs):
        return []

    def none(arrs):
        return

    return locals().get(method)(arrs)


def trans_value_to_threds(threds, value):
    threds = sorted(threds, key=lambda x: float(x))
    bigram = [(float(threds[n]), float(threds[n + 1])) for n in xrange(0, len(threds) - 1)]
    if len(threds) == 1:
        return threds[0], threds[0]
    for l, h in bigram:
        if l <= value < h:
            return l, h
    else:
        if value >= threds[-1]:
            return threds[-2], threds[-1]
        if value < threds[0]:
            return threds[0], threds[1]


class Conf(object):
    def __init__(self, name, method, ars):
        self.name = name
        self.method = method
        self.ars = ars

    def __str__(self):
        return "_".join([self.name, self.method, self.ars])


# FEA.get(FEA.fea_conf[fea_name].method)(fea_name,fea_value)

class Feature(object):
    feature_number = 0
    feature_dict = {}
    fea_id_out = os.path.join(root, "_".join([cst.app, "features_ids"]))
    fea_conf = {}
    fea_number_dict = {}
    config = config

    def __init__(self):
        # self.read_conf()
        pass

    def read_conf(self):
        with codecs.open(self.config, 'r', 'utf8') as f:
            for n, l in enumerate(f.readlines()):
                n += 1
                cf = Conf(*l.split(","))
                cf.arrs_list = gen_cates(cst.parse_method(cf.method)[0], cf.ars)
                self.fea_conf[cf.name] = cf
                self.fea_number_dict[cf.name] = n
        self.num_fea_dict = {self.fea_number_dict[x]: x for x in self.fea_number_dict}
        self.fea_number_value_list = {x: list() for x in self.fea_number_dict}

    def add_feature(self, fea_name):
        if fea_name in self.feature_dict:
            return self.feature_dict[fea_name]
        else:
            self.feature_number += 1
            self.feature_dict[fea_name] = self.feature_number

        return self.feature_number

    def cate(self, fea_name, fea_value):
        fea_value = "0" if fea_value == "NULL" else fea_value
        k = fea_name + "_" + fea_value
        return self.add_feature(k)

    def number(self, fea_name, fea_value):
        def wash(value):
            value = float(value)
            return  0 if value == "NULL" or value < 0 or value > 50000000 else value

        fea_value = wash(fea_value)
        cf = self.fea_conf[fea_name]
        threds = [float(x) for x in cf.ars.strip().split("#")]
        k = None
        l, h = trans_value_to_threds(threds, float(fea_value))
        k = "_".join(map(str, [fea_name, l, h]))
        return self.add_feature(k)

    def pair(self, fea_name_list, fea_value_list):
        print 1
        def check_value(fea_value):
            fea_value = fea_value.strip()
            return "0" if fea_value == "NULL" or fea_value <= "0" else fea_value

        fea_value_list = [check_value(val) for val in fea_value_list]
        # 获得参数list
        fea_list = fea_name_list
        cf_list = [self.fea_conf[fea] for fea in fea_list]
        # 做pair
        fea_name = "_".join([cf.name for cf in cf_list])

        def one_fea_value((cf, fea_value)):
            fea_value = float(fea_value.strip())
            data_method = cf.method.split("#")[0]
            if data_method == "number":
                print cf.arrs_list,fea_value
                print trans_value_to_threds(cf.arrs_list, fea_value)
                return "_".join([str(x) for x in trans_value_to_threds(cf.arrs_list, fea_value)])

            if data_method == "cate":
                return str(fea_value)

        value_name = "_".join(map(one_fea_value, zip(cf_list, fea_value_list)))
        key = "_".join([fea_name, value_name])
        return self.add_feature(key)

    def origin(self, fea_name, fea_value):
        k = fea_name
        if k in self.feature_dict:
            return self.feature_dict[k]
        else:
            self.feature_number += 1
            self.feature_dict[k] = self.feature_number
        return self.feature_number

    def print_feas(self):
        with codecs.open(self.fea_id_out, 'w', 'utf8') as f:
            f.write("\n".join(
                    sorted([k.strip() + "&#&" + str(v).strip() for k, v in self.feature_dict.items()],
                           key=lambda x: x.split("&#&")[1])))


if __name__ == "__main__":
    FEA = Feature()
    FEA.read_conf()
