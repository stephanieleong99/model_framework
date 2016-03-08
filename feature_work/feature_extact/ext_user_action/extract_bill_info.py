# encoding:utf8
import sys
import multiprocessing
import collections
import itertools
import codecs
import optparse
from optparse import OptionParser
from collections import defaultdict
import gc

data_path = '/Users/dongjian/data/user_action'
out_path = '/Users/dongjian/data/user_action_out'

SEP = '\t'
TEST = "test"
ONLINE = "online"

NUM_WORKERS = 10

input_list = ["uuid",
              "time",
              "wm_ctime",
              "page_url",
              "customerId",
              "dId"]

action_list = ["poi/filter", "home/head", "poi/food/", "order/preview", "poi/search/foodpoi",
               "poi/getfilterconditions", "poicoupons/number", "order/update", "order/submit", "order/status",
               "Order/detail", "order/getuserorders", "order/getfoodlist", "/address/getaddr",
               "share/envelope"]


def find_action(raw):
    for act in action_list:
        if act in raw:
            return act
    else:
        return None


def init_arguments():
    def right_mode(p):
        if p == TEST or p == ONLINE:
            return p
        else:
            raise optparse.OptionValueError('%s is not a mode.' % p)

    parser = optparse.OptionParser()
    parser.add_option('-m', type=str, dest='mode',
                      help="work mode")
    return parser.parse_args()


class OrderInput(object):
    def __init__(self, line):
        l = line.strip().split(SEP)

        def create((num, value)):
            self.__dict__[input_list[num]] = l[num]

        map(create, [(n, v) for n, v in enumerate(l)])

    def __str__(self):
        return '\n'.join([":".join([k, v]) for k, v in self.__dict__.items()])


class Action(object):
    '''
    url 的抽象. 包含action的属性.
    '''
    name = None
    time = None  # 时间戳
    para = None
    order_id = None

    def __init__(self, raw, time, uuid, para=None):
        self.name = find_action(raw)
        self.raw = raw
        self.time = time
        self.uuid = uuid
        self.para = para

    @classmethod
    def construct(cls, inp):
        act = cls(*[inp.page_url, inp.time, inp.uuid])
        return act

    def __str__(self):
        return "\t".join(map(str, [self.name, self.time, self.para, self.order_id, self.raw]))


class OrderActions(object):
    '''
    order 的动作合集,一组action list, 包括订单的一些特征.

    如何分割订单?

    '''
    order_id = None
    order_act = None
    uuid = None
    raw_action_list = None
    action_list = None  # action 按时间戳排序的动作合集
    start_time = None
    submit_time = None
    end_time = None

    # feature
    time_before_submit = None
    visit_pois_before_submit = None
    search_times = None

    # is_share = None

    def __init__(self, action_list):
        self.action_list = action_list

    def __str__(self):
        return "\t".join(
                map(str,
                    [self.order_id, self.uuid, self.poi_id, self.start_time, self.submit_time, self.time_before_submit,
                     self.visit_pois_before_submit, self.search_times]))
        # @classmethod
        # def

    def cal_order_basic_info(self):
        order = self.order_act.raw.split("/")
        self.poi_id = order[-2]
        self.order_id = order[-1]
        self.start_time = self.pure_action_list[0].time
        self.uuid = self.order_act.uuid

    def pure_actions(self):
        self.submit_time = float(self.order_act.time)

        def pure(action):
            if self.submit_time - float(action.time) < 3600:
                return True

        self.pure_action_list = filter(pure, self.action_list)

    def cal_features(self):
        self.time_before_submit = float(self.pure_action_list[-1].time) - float(self.pure_action_list[0].time)

        def sum_poi(action_list):
            def one_poi(action):
                if "poi/food" in action.raw:
                    return action.raw.split("/")[-2]

            return list(set(filter(lambda x: x, map(one_poi, self.pure_action_list))))

        self.visit_pois_before_submit = len(sum_poi(self.pure_action_list))

        self.search_times = len([x for x in self.pure_action_list if "search" in x.raw])

    def cal_things(self):

        self.pure_actions()
        self.cal_order_basic_info()
        self.cal_features()

@profile
def generate_data(mode):
    # online
    if mode == ONLINE:
        for line in sys.stdin:
            yield line
    # test
    if mode == TEST:
        with codecs.open(data_path, 'r', 'utf8') as f:
            for l in f.readlines():
                yield l


def aggr_act_to_order(inputs):
    '''
    print
    :return:
    '''

    actions = filter(lambda act: act.name, [Action.construct(x) for x in inputs])
    actions = sorted(actions, key=lambda x: int(x.time))

    def aggr(actions):
        tmp_list = []
        for act in actions:
            if act.name == "order/submit":
                tmp_list.append(act)
                order_actions = OrderActions(tmp_list)
                order_actions.action_list = tmp_list
                order_actions.order_act = act
                order_actions.uuid = act.uuid
                yield order_actions
                tmp_list = []
            else:
                tmp_list.append(act)
                # print len(tmp_list),'af'

    return list(aggr(actions))


def split_orders(order_inputs):
    """
    uuid_inputs is a dict
    分割单个uuid 对应的order_inputs,返回order_actions.
    :param order_inputs:
    :return:
    """
    orders = filter(lambda x: x, aggr_act_to_order(order_inputs))
    return orders

    # 聚合act到order

@profile
def generate_orderinput(mode):
    tmp_list = []
    tmp_uuid = None
    i = 0
    for l in generate_data(mode):
        inp = OrderInput(l)
        if inp.uuid != tmp_uuid:
            # print i
            if tmp_list:
                i += 1
                yield tmp_list
            tmp_list = []
            tmp_list.append(inp)
            tmp_uuid = inp.uuid
        else:
            tmp_list.append(inp)


def split_order_inputs_by_uuid(mode):
    # uuid_inputs = defaultdict(list)
    for order_inputs in generate_orderinput(mode):
        # map(lambda inp: uuid_inputs[inp.uuid].append(inp), order_inputs)
        yield order_inputs


def print_orders(orders):
    print "\n".join([str(ord) for ord in orders])


@profile
def pipeline(mode):
    for order_inputs in split_order_inputs_by_uuid(mode):
        orders = filter(lambda x: x, split_orders(order_inputs))
        if orders:
            map(lambda ord: ord.cal_things(), orders)
            print_orders(orders)
        del orders
        gc.collect()
        # import time
        # time.sleep(0.1)
        # from guppy import hpy
        # h = hpy()
        # print h.heap()


'''
1 读入所有数据.
2 分割为uuidwc
3 分割为订单. 返回的是order_actions.
4 输出订单信息最后
'''

'''
支持分割订单.
提下单前平均访问时间
下单前平均访问商家


用户产出:

提下单前平均访问时间
下单前平均访问商家
各url 访问次数
下单平均时间间隔
开始阶段访购率
'''

if __name__ == "__main__":
    args = init_arguments()[0]
    pipeline(args.mode)
