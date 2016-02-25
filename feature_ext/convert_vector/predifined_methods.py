# encoding:utf8
import os
import codecs

from utils.CONST import root
import json

config = '/Users/dongjian/PycharmProjects/UserDetected/feature_ext/convert_vector/test/feas'


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
    fea_id_out = os.path.join(root, "features_ids")
    fea_conf = {}
    fea_number_dict = {}
    config = '/Users/dongjian/PycharmProjects/UserDetected/feature_ext/convert_vector/test/feas'

    def __init__(self):
        # self.read_conf()
        pass

    def read_conf(self):
        with codecs.open(self.config, 'r', 'utf8') as f:
            for n, l in enumerate(f.readlines()):
                n += 1
                cf = Conf(*l.split(","))
                self.fea_conf[cf.name] = cf
                self.fea_number_dict[cf.name] = n
        self.num_fea_dict = {self.fea_number_dict[x]: x for x in self.fea_number_dict}
        self.fea_number_value_list = {x: list() for x in self.fea_number_dict}

    def cate(self, fea_name, fea_value):
        fea_value = "0" if fea_value == "NULL" else fea_value
        k = fea_name + "_" + fea_value
        if k in self.feature_dict:
            return self.feature_dict[k]
        else:
            self.feature_number += 1
            self.feature_dict[k] = self.feature_number
        return self.feature_number

    def number(self, fea_name, fea_value):
        fea_value = 0 if fea_value == "NULL" or fea_value <= 0 else fea_value

        cf = self.fea_conf[fea_name]

        print str(cf)
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
        if k in self.feature_dict:
            return self.feature_dict[k]
        else:
            self.feature_number += 1
            self.feature_dict[k] = self.feature_number

        return self.feature_number

    # def pair(self, fea_name, fea_value):
    #     fea_list = fea_name.split("&")
    # 
    #     def one_

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
    f = Feature()
    f.read_conf()
