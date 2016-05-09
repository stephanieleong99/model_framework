# encoding:utf8
import codecs
import itertools
import os

import utils.CONST as cst
from utils.CONST import DATA_DIR

# 特征处理配置信息
FEA_CONFIG = os.path.join(cst.APP_PATH, 'feas')
GOLD_SPLIT = "__"
FEA_ID_SPLIT = "&#&"
FEA_SPILT = "&"


def gen_arr_list_by_method_name(method, arrs):
    """根据不同特征处理方法解析参数列表"""
    def cate(arrs):
        return {ar: 1 for ar in arrs.split("#")}

    def number(arrs):
        # if len(arrs) <= 1: return []
        # method_arr_list = [float(x) for x in arrs.strip().split("#")]
        return [float(x) for x in arrs.strip().split("#")] if len(arrs) > 1 else []

    def pair(arrs):
        return []

    def none(arrs):
        return

    def origin(arrs):
        # if len(arrs) <= 1: return []
        # method_arr_list = [float(x) for x in arrs.strip().split("#")]
        return [float(x) for x in arrs.strip().split("#")] if len(arrs) > 1 else []

    return locals().get(method)(arrs)


def trans_value_to_threds(inp_list, tgt, lo=0, hi=None):
    if lo < 0:
        raise ValueError("lo must be non-negative")
    if hi is None:
        hi = len(inp_list)
    if tgt > inp_list[hi - 1] or tgt < inp_list[lo]:
        return None
    if tgt == inp_list[lo]:
        return inp_list[0], inp_list[0]
    if tgt == inp_list[hi - 1]:
        return inp_list[hi - 2], inp_list[hi - 1]

    while lo < hi:
        mid = (lo + hi) // 2
        if inp_list[mid] < tgt:
            lo = mid + 1
        else:
            hi = mid

    return inp_list[lo - 1], inp_list[lo]


class Conf(object):
    """单个特征或特征组合的处理配置信息"""
    def __init__(self, name, method, filter_threds, status, args):
        self.name = name
        self.method = method
        self.status = status.lower() in ("yes", "true", "t", "1")
        self.filter_threds = filter_threds.split("#") if filter_threds else None
        self.args = args.strip()
        self.value_list = None
        self.feature_list = None
        self.key = None
        self.pre_cal()

    def pre_cal(self):
        # precalculate
        self.method_arr_list = gen_arr_list_by_method_name(cst.parse_method(self.method)[0], self.args)

    def __str__(self):
        return "_".join(map(str, [self.name, self.method, self.status, self.args]))

    def parse_args(self, conf_dict):
        def number(args):
            self.value_list = args.split("#")
            self.feature_list = ["_".join([self.value_list[ar], self.value_list[ar + 1]]) for ar in
                                 xrange(0, len(self.value_list) - 1)]
            self.key_list = ["__".join([self.name, fea_value]) for fea_value in self.feature_list]

        def cate(ars):
            self.value_list = ars.split("#")
            self.feature_list = self.value_list
            self.key_list = ["__".join([self.name, fea_value]) for fea_value in self.feature_list]

        def pair(args):
            fea_name_list = self.name.split("&")
            fea_key_list = [conf_dict[name].key_list for name in fea_name_list]
            product_list = itertools.product(*fea_key_list)
            self.key_list = ["#".join(x) for x in product_list]
            self.name = "#".join(fea_name_list)

        def none(args):
            pass

        def origin(args):
            self.key_list = [self.name]

        locals().get(self.method.split("#")[0])(self.args)


class Feature(object):
    """所有特征的信息"""
    feature_number = 0
    feature_dict = {} # name: id
    # /Users/lt/data/user_features_v_1_test_features_ids: feature1#feature2#...&#&id
    fea_id_out = os.path.join(DATA_DIR, "_".join([cst.APP, "features_ids"]))
    # name: conf
    name_conf_dict = {}
    name_id_dict = {} # name: id
    config = FEA_CONFIG
    lock = True

    def __init__(self):
        # self.read_conf()
        self._init_feature_dict()

    def _init_feature_dict(self):
        # 第一次调用时,fea_id_out文件不存在
        if os.path.isfile(self.fea_id_out):
            with codecs.open(self.fea_id_out) as file:
                def one_fea(data):
                    fea_name, id = data.split(FEA_ID_SPLIT)
                    self.feature_dict[fea_name.strip()] = id.strip()

                map(one_fea, file.readlines())
            try:
                self.feature_number = int(sorted(self.feature_dict.values(), key=lambda x: int(x), reverse=True)[0])
            except  Exception as e:
                print e

    def read_conf(self):
        with codecs.open(self.config, 'r', 'utf8') as f:
            for n, l in enumerate(f.readlines()):
                if not l.startswith("#"):  # 过掉注释
                    n += 1
                    cf = Conf(*l.split(","))
                    cf.method_arr_list = gen_arr_list_by_method_name(cst.parse_method(cf.method)[0], cf.args)
                    self.name_conf_dict[cf.name] = cf
                    self.name_id_dict[cf.name] = n
        self.id_name_dict = {self.name_id_dict[x]: x for x in self.name_id_dict}
        # 初始化特征的value list
        self.name_values_dict = {x: list() for x in self.name_id_dict}

    def add_feature(self, fea_name, fea_value=1):
        # print fea_name,fea_name in self.feature_dict

        if fea_name in self.feature_dict:
            return self.feature_dict[fea_name], fea_value
        elif not self.lock:
            self.feature_number += 1
            self.feature_dict[fea_name] = self.feature_number
        else:
            return None
        return self.feature_number, fea_value

    def cate(self, fea_name, fea_value):
        fea_value = "0" if fea_value == "NULL" else fea_value
        k = fea_name + GOLD_SPLIT + fea_value
        return self.add_feature(k)

    def number(self, fea_name, fea_value):
        def wash(value):
            if value == "NULL":
                value = 0
            value = float(value)
            return 0 if value < 0 or value > 50000000 else value

        fea_value = wash(fea_value)
        cf = self.name_conf_dict[fea_name]
        rs = trans_value_to_threds(cf.method_arr_list, float(fea_value))
        if rs:
            l, h = rs
            k = GOLD_SPLIT.join([fea_name, "_".join(map(lambda x: str(float('%0.3f' % float(x))), [l, h]))])
            return self.add_feature(k)
        else:
            return None

    def pair(self, fea_name_list, fea_value_list):

        def check_value(fea_value):
            fea_value = fea_value.strip()
            return "0" if fea_value == "NULL" or fea_value <= "0" else fea_value

        fea_value_list = [check_value(val) for val in fea_value_list]
        # 获得参数list
        fea_list = fea_name_list
        cf_list = [self.name_conf_dict[fea] for fea in fea_list]
        # 做pair
        fea_name_list = [cf.name for cf in cf_list]

        def one_fea_value((cf, fea_value)):
            fea_value = fea_value.strip()
            data_method = cf.method.split("#")[0]
            if data_method == "number":
                fea_value = float(fea_value)
                rs = trans_value_to_threds(cf.method_arr_list, fea_value)
                if rs:
                    return "_".join([str(x) for x in rs])

            if data_method == "cate":
                return str(fea_value)

        value_name_list = map(one_fea_value, zip(cf_list, fea_value_list))

        def check_value_list(inp_list):
            return min(inp_list) != None

        if check_value_list(value_name_list):
            key = "#".join(["__".join([fea, value]) for fea, value in zip(fea_name_list, value_name_list)])
            return self.add_feature(key)

    def origin(self, fea_name, fea_value):
        k = fea_name

        cf = self.name_conf_dict[fea_name]
        l, h = map(lambda x: float('%0.3f' % float(x)), cf.method_arr_list)
        fea_value = cst.wash(fea_value)
        fea_value = float('%0.3f' % float(fea_value))
        if l <= fea_value <= h:
            return self.add_feature(fea_name, fea_value)

    def print_feas(self):
        with codecs.open(self.fea_id_out, 'wb', 'utf8') as f:
            f.truncate()
            f.write("\n".join(
                    sorted([k.strip() + "&#&" + str(v).strip() for k, v in self.feature_dict.items()],
                           key=lambda x: x.split("&#&")[1])))