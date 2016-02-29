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
        if len(arrs) <=1: return []
        arrs_list = [float(x) for x in arrs.split("#")]
        return arrs_list

    def pair(arrs):
        return []
    print method,"gen"
    return locals().get(method)(arrs)


def trans_value_to_threds(threds, value):
    for l, h in [(float(threds[n]), float(threds[n + 1])) for n in xrange(0, len(threds) - 1)]:
        if l <= value < h:
            return l, h
    if value == threds[-1] and len(threds) > 1:
        return threds[-2], threds[-1]
    else:
        return threds[0], threds[0]


class Conf(object):
    def __init__(self, name, method, ars):
        self.name = name
        self.method = method
        self.ars = ars

    def __str__(self):
        return "__".join([self.name, self.method, self.ars])


# FEA.get(FEA.fea_conf[fea_name].method)(fea_name,fea_value)

class Feature(object):
    feature_number = 0
    feature_dict = {}
    fea_id_out = os.path.join(root,"_".join([cst.app,"features_ids"]))
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
        fea_value = 0 if fea_value == "NULL" or fea_value <= 0 else fea_value
        cf = self.fea_conf[fea_name]
        threds = [float(x) for x in cf.ars.strip().split("#")][::-1]
        k = None
        try:
            for n, t in enumerate(threds):
                if float(fea_value) >= t:
                    k = "_".join([fea_name, str(threds[n]), str(threds[n - 1] if n - 1 >= 0 else "end")])
                    # print "len(threds):",threds,"n:",n,"t:",t,"fea_value:",fea_value,'k:',k
                    break
        except Exception as e:
            k = "_".join([fea_name, "-1"])
        if not k:
            k = "_".join([fea_name, "-1"])
        return self.add_feature(k)

    def pair(self, fea_name_list, fea_value_list):
        def check_value(fea_value):
            fea_value = fea_value.strip()
            return "0" if fea_value == "NULL" or fea_value <= "0" else fea_value

        print fea_name_list, fea_value_list, '1'
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
                print cf.arrs_list, fea_value, '4'
                return "_".join([str(x) for x in trans_value_to_threds(cf.arrs_list, fea_value)])

            if data_method == "cate":
                return str(fea_value)

        value_name = "_".join(map(one_fea_value, zip(cf_list, fea_value_list)))
        print fea_name, value_name, '2'
        key = "_".join([fea_name, value_name])
        print key, "key###########", '3'
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
