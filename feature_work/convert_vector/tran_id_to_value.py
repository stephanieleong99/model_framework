# encoding:utf8
import codecs
import os
import math
from utils.CONST import DATA_DIR as root
import utils.CONST as cst
import multiprocessing as mp
import re

# /Users/lt/data/user_features_v_1_test_features_ids
feature_ids_path = os.path.join(root, "_".join([cst.APP, "features_ids"]))
# /Users/lt/data/model_xishu_ids_sort
model_xishu_ids_sort = os.path.join(root, "model_xishu_ids_sort")
# /Users/lt/data/user_features_v_1_test_features_lines: label	feature_id:feature_value ...
feature_lines_path = os.path.join(root, "_".join([cst.APP, "features_lines"]))
# /Users/lt/data/user_features_v_1_test_features_lines_with_info
feature_lines_with_info = os.path.join(root, "_".join([cst.APP, "features_lines_with_info"]))

# label	feature_id:feature_value feature_id:feature_value ...
with codecs.open(feature_lines_path, 'r', 'utf8') as f:
    def one_fea(fea):
        fsp = fea.split('\t')
        # 只取出值为1的feature的id
        label, features = fsp[0], [key.split(":")[0] for key in fsp[1].split(" ") if key.split(":")[1]=="1"]
        return label, features
    feature_lines = map(one_fea, f.readlines()[:])
    # print feature_lines

# feature_name#feature_name#...&#&feature_id    arg1 arg2 ...
with codecs.open(model_xishu_ids_sort, 'r', 'utf8') as f:
    def one_conf(data):
        # split为[feature_name#feature_name#..., feature_id, arg1 arg2 ...]
        return map(lambda x: str(x).strip(), re.split("&#&|\t", data))
    # 过滤出有参数的特征(组合)
    tuple = filter(lambda x: len(x) > 2 and len(x[2]) > 0, map(one_conf, f.readlines()))
    # key为feature_ids
    key_dict = {t[1]: (t[0], t[1], t[2]) for t in tuple}
    # key为feature_names
    name_dict = {t[0]: (t[0], t[1], t[2]) for t in tuple}
    # import json
    # print json.dumps(key_dict,indent=4)
    # print len(key_dict)

with codecs.open(feature_lines_with_info, 'w', 'utf8') as f:
    # data ==> label, [feature_id, feature_id, ...]
    def one_line(data):
        # ele: [feature_id, feature_id, ...]
        def one_ele(ele):
            val = key_dict.get(ele, None)
            if val :  # val 不为空 且 系数不为0
                return "___".join([val[0], val[-1]])

        return "\t".join(
                sorted(filter(lambda x: x, map(one_ele, data[1])), key=lambda x: -float(x.split("___")[1])))
    import multiprocessing as mp
    pool = mp.Pool(32)
    rs = pool.map(one_line, feature_lines)
    # print rs[0]
    f.write('\n'.join(rs))
    print len(feature_lines),len(rs)
    print feature_lines[-1],rs[-1]
