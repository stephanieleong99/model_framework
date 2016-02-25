package com.meituan.waimairc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}


/**
 * computeEntropy
 *
 */
case class personEntropy(
                            recipient_phone: String,
                            entropy: Double
                            )

object computeEntropy {
    def normalization(data: Array[Double], data_stat_max: Array[Double], data_stat_min: Array[Double]): Array[Double] ={
        val result = new Array[Double](data.length)
        for(i <- 0 to data.length-1){
            val tmp = data_stat_max.apply(i) - data_stat_min.apply(i)
            if(tmp == 0)
                result(i)=((data.apply(i) - data_stat_min.apply(i))*1.0/1)
            else
                result(i)=((data.apply(i) - data_stat_min.apply(i))*1.0/(data_stat_max.apply(i) - data_stat_min.apply(i)))
        }
        result
    }

    def sum_data(x: Array[Double], y: Array[Double]): Array[Double] ={
        val result = new Array[Double](x.length)
        for(i <- 0 to x.length - 1){
            result(i) = x(i) + y(i)
        }
        result
    }


    def compute_entropy(data: Array[Double], data_normalization_sum: Array[Double]): Array[Double] ={
        val result = new Array[Double](data.length)
        for(i <- 0 to data.length - 1){
            if(data_normalization_sum.apply(i) == 0.0) {
                val f = data(i) * 1.0 / 1
                result(i) = f * math.log1p(f)
            }else{
                val f = data(i) * 1.0 / data_normalization_sum.apply(i)
                result(i) = f * math.log1p(f)
            }
        }
        result
    }

    def compute_result(data: Array[Double], w_r:Array[Double], w_2: Array[Double]): Double = {
        var sum = 0.0
        for(i <- 0 to data.length - 1){
            sum = sum + data(i) * w_r.apply(i)*w_2.apply(i)
        }
        sum
    }

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf())
        val sqlContext = new HiveContext(sc)
        import sqlContext.implicits._

        val user_data = sqlContext.sql("select recipient_phone, 1hour2ord_days , 7day14ord_days , food_continus_days ,  outofrange_days, max_continue_order_num, all_order_ratio, aver_order, all_time_before_120s, " +
            "start_time_before_120s, start_order_ratio  from mart_waimai_risk.dim_risk_user__bayes_phone_blacklist")
        //val user_data = sc.textFile("/Users/lishuyi/个人工作记录/工作文档/风控/数据相关/大盘数据分析/data.txt")
        val data_set = user_data.map(line => (line.get(0).toString, Array(line.get(1).toString.toDouble,
            line.get(2).toString.toDouble, line.get(3).toString.toDouble, line.get(4).toString.toDouble,
            line.get(5).toString.toDouble, {
            if(line.get(6).toString.toDouble>1.2)
            0
            else
                line.get(6).toString.toDouble}, line.get(7).toString.toDouble, line.get(8).toString.toDouble,
            {if(line.get(9).toString.toDouble<0)
                0
                else line.get(9).toString.toDouble}, {if(line.get(10).toString.toDouble>1.2) 0 else line.get(10).toString.toDouble})))
        /*val data_set = user_data.map(line => (line.split("\t")(0).toInt, Array(line.split("\t")(1).toDouble, line.split("\t")(2).toDouble,
            line.split("\t")(3).toDouble, line.split("\t")(4).toDouble, line.split("\t")(5).toDouble)))*/
        val data_vector = data_set.map(f=> Vectors.dense(f._2))
        val data_stat = Statistics.colStats(data_vector)
        // 广播变量视数据量情况考虑是否使用
        val data_stat_max = sc.broadcast(data_stat.max)
        val data_stat_min = sc.broadcast(data_stat.min)

        val data_normalization = data_set.map(line => {
            val recipeint_phone = line._1
            val data = line._2
            val value = normalization(data, data_stat.max.toArray, data_stat.min.toArray)
            (recipeint_phone, value)
        })
        val data_normalization_sum = data_normalization.map(line => line._2).reduce(sum_data(_,_))
        val data_entropy = data_normalization.map(line => {
            val data = line._2
            val value = compute_entropy(data, data_normalization_sum)
            value
        }).reduce(sum_data(_,_))
        val H = Array(-1.0 * data_entropy(0)/math.log1p(10), -1.0 * data_entropy(1)/math.log1p(10), -1.0 * data_entropy(2)/math.log1p(10),
            -1.0 * data_entropy(3)/math.log1p(10), -1.0 * data_entropy(4)/math.log1p(10), -1.0 * data_entropy(5)/math.log1p(10),
            -1.0 * data_entropy(6)/math.log1p(10), -1.0 * data_entropy(7)/math.log1p(10), -1.0 * data_entropy(8)/math.log1p(10),
            -1.0 * data_entropy(9)/math.log1p(10))
        val w = Array((1 - H(0))/(10 - H.sum), (1 - H(1))/(10 - H.sum),(1 - H(2))/(10 - H.sum),(1 - H(3))/(10 - H.sum),(1 - H(4))/(10 - H.sum),
            (1 - H(5))/(10 - H.sum),(1 - H(6))/(10 - H.sum),(1 - H(7))/(10 - H.sum),(1 - H(8))/(10 - H.sum),(1 - H(9))/(10 - H.sum))
        val w_2 = Array(0.082191232, 0.090746478, 0.234435965, 0.031814934, 0.015012228, 0.090746478, 0.033712187, 0.090746478,
            0.234435965, 0.096158055)
        val result = data_normalization.map(line => {
            val recipient_phone = line._1
            val entropy = compute_result(line._2, w,w_2)
            personEntropy(recipient_phone, entropy*1000)
        })

        result.cache()
        result.toDF("recipient_phone","entropy").registerTempTable("user_entropy_test")

        sqlContext.sql("use mart_waimai_risk")
        sqlContext.sql("set hive.exec.dynamic.partition=true")
        sqlContext.sql("set hive.exec.dynamic.partition.mode=nostrick")
        sqlContext.sql("drop table mart_waimai_risk.dim_risk_user__bayes_phone_blacklist_entropy")
        sqlContext.sql("create table if not exists mart_waimai_risk.dim_risk_user__bayes_phone_blacklist_entropy(recipient_phone String, entropy Double)")
        sqlContext.sql("insert overwrite table dim_risk_user__bayes_phone_blacklist_entropy select recipient_phone, entropy from user_entropy_test")
    }
}