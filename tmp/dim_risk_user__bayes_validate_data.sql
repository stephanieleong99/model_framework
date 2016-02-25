##Description##
##-- 这个节点填写本ETL的描述信息, 包括目标表定义, 建立时的需求jira编号等

##TaskInfo##
creator = 'lishuyi@meituan.com'

source = {
    'db': META['hmart_waimai_risk'],
}

stream = {
    'format': 'recipient_phone, ord_num, last_date, is_food_continus,food_continus_days,food_continus_weight, food_continus_rate, is_1hour2ord, 1hour2ord_days, 1hour2ord_weight, 1hour2ord_rate, is_7day14ord,7day14ord_days, 7day14ord_weight, 7day14ord_rate, is_reback, reback_score, reback_weight, reback_rate',
}

target = {
    'db': META['hmart_waimai_risk'],
    'table': 'dim_risk_user__bayes_validate_data',
}

##Load##
##-- Load节点, (可以留空)
set hive.exec.dynamic.partition=true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table `$target.table`
select
	a.recipient_phone,
	a.last_date,
	a.is_food_continus,
	a.food_continus_days,
	a.food_continus_weight,
	a.is_1hour2ord,
	a.1hour2ord_days,
	a.1hour2ord_weight,
	a.is_7day14ord,
	a.7day14ord_days,
	a.7day14ord_weight,
	a.is_outofrange,
	a.outofrange_days,
	a.outofrange_weight,
	a.max_continue_order_num,
	a.is_max_continue_order_num,
	a.all_order_ratio,
	a.is_all_order_ratio,
	a.aver_order,
	a.is_aver_order,
	a.all_time_before_120s,
	a.is_all_time_before_120s,
	a.start_time_before_120s,
	a.is_start_time_before_120s,
	a.start_order_ratio,
	a.is_start_order_ratio,
  	a.type,
	(a.food_continus_days*a.food_continus_weight+a.1hour2ord_days*a.1hour2ord_weight+a.7day14ord_days*a.7day14ord_weight+a.outofrange_days*a.outofrange_weight+a.max_continue_order_num+a.all_order_ratio+a.aver_order+a.all_time_before_120s+a.start_time_before_120s+a.start_order_ratio),
	concat(a.type,',',a.food,' ',a.hour, ' ', a.week, ' ', a.rangeout,' ', a.is_max_continue_order_num, ' ', a.is_all_order_ratio, ' ', a.is_aver_order, ' ', a.is_all_time_before_120s, ' ', a.is_start_time_before_120s, ' ', a.is_start_order_ratio)
from
(
select
	a.recipient_phone,
	a.last_date,
	if(coalesce(b.recipient_phone,-1)=-1,0,1) as is_food_continus,
	coalesce(b.food_continus_days,0) food_continus_days,
  	if(coalesce(b.food_continus_days,0)>=3,'1','0') as food,
	1 as food_continus_weight,
	if(coalesce(b.recipient_phone,-1)=-1,0,1) as is_1hour2ord,
	coalesce(b.1hour2ord_days,0) 1hour2ord_days,
  	if(coalesce(b.1hour2ord_days,0)>=2,'1','0') as hour,
	1 as 1hour2ord_weight,
	if(coalesce(b.recipient_phone,-1)=-1,0,1) as is_7day14ord,
	coalesce(b.7day14ord_days,0)	7day14ord_days,
  	if(coalesce(b.7day14ord_days,0)>=2,'1','0') as week,
	1 as 7day14ord_weight,
	if(coalesce(b.recipient_phone,-1)=-1,0,1) as is_outofrange,
	coalesce(b.outofrange_days,0)	outofrange_days,
  	if(coalesce(b.outofrange_days,0)>=3,'1','0') as rangeout,
	1 as outofrange_weight,
  	if(b.max_continue_order_num>=6,'1','0') as is_max_continue_order_num,
  	coalesce(b.max_continue_order_num,0) as max_continue_order_num,
 	if(b.all_order_ratio>0.7,'1','0') as is_all_order_ratio,
  	coalesce(b.all_order_ratio, 0) as all_order_ratio,
 	if(b.aver_order>7,'1','0') as is_aver_order,
  	coalesce(b.aver_order,0) as aver_order,
 	if(b.all_time_before_120s>0.5,'1','0') as is_all_time_before_120s,
  	coalesce(b.all_time_before_120s,0) as all_time_before_120s,
 	if(b.start_time_before_120s>0.4,'1','0') as is_start_time_before_120s,
  	coalesce(b.start_time_before_120s,0) as start_time_before_120s,
 	if(b.start_order_ratio>0.7,'1','0') as is_start_order_ratio,
  	coalesce(b.start_order_ratio,0) as start_order_ratio,
  	type
from
(
	select
	a.phone as recipient_phone, a.last_date,  a.ord_num,'0' as type
from
	mart_waimai_risk.dim_user_property_risk a
  where
  	a.last_date>='20150901' and punish_status=0
) a
left outer join
(
	select
  		b.recipient_phone,
  		max(a.food_continus_days) as food_continus_days,
  		max(a.1hour2ord_days) as 1hour2ord_days,
  		max(a.7day14ord_days) as 7day14ord_days,
  		max(a.outofrange_days) as outofrange_days,
  		max(a.max_continue_order_num) as max_continue_order_num,
  		max(a.all_order_ratio) as all_order_ratio,
  		max(a.aver_order) as aver_order,
  		max(a.all_time_before_120s) as all_time_before_120s,
  		max(a.start_time_before_120s) as start_time_before_120s,
  		max(a.start_order_ratio) as start_order_ratio
  	from
  		mart_waimai_risk.aggr_dapan_analysis__usr_risk a
  	inner join
  	(
    	select
      		user_id,
      		recipient_phone
      	from
      		mart_waimai_risk.fact_act_base_risk
      	where
      		dt>='20150601'
      	group by
      		user_id,recipient_phone
    ) b
  	on
  		a.user_id = b.user_id
  	group by
  		b.recipient_phone
)b
on
  a.recipient_phone = b.recipient_phone
) a
left outer join
(select
 	phone
 from
	mart_waimai_risk.dim_user_whitelist_risk
 where
 	is_tuancan=1 or is_discount_low=1
) b
on
	a.recipient_phone = b.phone
where
	coalesce(b.phone,-1)=-1
;

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`(
  	recipient_phone string COMMENT '手机号',
  	last_date string COMMENT '最后出现日期',
  	is_food_continus int COMMENT '菜品连续策略命中：0未命中，1命中',
  	food_continus_days int COMMENT '菜品连续策略评分',
  	food_continus_weight double COMMENT '菜品连续策略权重',
  	is_1hour2ord int COMMENT '1小时2单：0未命中，1命中',
  	1hour2ord_days int COMMENT '1小时2单打分',
  	1hour2ord_weight double COMMENT '1小时2单权重',
  	is_7day14ord int COMMENT '7天14单：0未命中，1命中',
  	7day14ord_days int COMMENT '7天14单打分',
  	7day14ord_weight double COMMENT '7天14单权重',
  	is_outofrange int COMMENT '不在配送范围内集中下单：0未命中，1命中',
  	outofrange_days int COMMENT '不在配送范围内集中下单打分',
  	outofrange_weight double COMMENT '不在配送范围内集中下单权重',
  	max_continue_order_num int COMMENT '',
  	is_max_continue_order_num int COMMENT '',
  	all_order_ratio double COMMENT '',
  	is_all_order_ratio int COMMENT '',
  	aver_order double COMMENT '',
  	is_aver_order int COMMENT '',
  	all_time_before_120s double COMMENT '',
  	is_all_time_before_120s int COMMENT '',
  	start_time_before_120s double COMMENT '',
  	is_start_time_before_120s int COMMENT '',
  	start_order_ratio double COMMENT '',
  	is_start_order_ratio int COMMENT '',
  	type string COMMENT '',
  	total_score double COMMENT '总评分',
  	data string COMMENT ''
) COMMENT '贝叶斯验证数据'
stored as orc