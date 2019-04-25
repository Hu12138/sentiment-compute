package com.shujia.realtime

import com.shujia.util.SparkTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, HConnectionManager, Put}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.Jedis

object WeiboCommentStreaming extends SparkTool {
  /**
    * spark配置初始化方法，初始化conf对象
    */
  override def init(args: Array[String]): Unit = {
    conf.setMaster("local[4]")
  }


  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    *
    * @param args
    */
  override def run(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val ssc = new StreamingContext(sc, Durations.seconds(5))
    ssc.checkpoint("checkpoint")
    /**
      *
      * {"article_id": "4361619838038416",
      * "sentiment_id": "4",
      * "score": 0.98,
      * "comment_id": "4361622996391575",
      * "created_at": "2019-04-16 08:43:40",
      * "user_name": "戳泡泡的豆子甜",
      * "user_id": 1702868900,
      * "total_number": 0,
      * "like_count": 7,
      * "text": "不得不说这是在巴黎 浪漫的城市"}
      *
      */
    val DS = KafkaUtils.createStream(
      ssc,
      "node1:2181,node2:2181,node3:2181",
      "comment1",
      Map("WeiBoCommentTopic" -> 4)
    )


    /**
      * 实时统计情感走势
      */
    DS
      .map(_._2)
      .transform(rdd => {
        val DF = sqlContext.read.json(rdd)
        DF.rdd
      })
      .mapPartitions(i => {
        val conf = new Configuration
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181")
        val connection = HConnectionManager.createConnection(conf)

        val table = connection.getTable("commentDistinct")

        //对数据做去重处理，根据评价id去重
        val data = i.filter(row => {
          val comment_id = row.getAs[String]("comment_id")
          val get = new Get(comment_id.getBytes())

          var flag = true

          //如果key已经存在过滤该数据
          if (table.exists(get)) {
            flag = false
          } else {
            val put = new Put(comment_id.getBytes())
            put.add("i".getBytes(), "1".getBytes(), "1".getBytes())
            table.put(put)
          }
          flag
        })

        table.close()
        connection.close()

        data
      })

      .map(row => {
        val sentiment_id = row.getAs[String]("sentiment_id")
        val created_at = row.getAs[String]("created_at")
        val score = row.getAs[Double]("score")
        val newScorce = if (score < 0.4) 0 else if (score > 0.6) 2 else 1

        (sentiment_id + "\t" + created_at.substring(0, 11) + "\t" + newScorce, 1)
      })

      //统计每个舆情每个时间正负情感数
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(i => {
          //创建redis  连接
          //创建连接
          val jedis = new Jedis("node1", 6379)
          i.foreach(line => {
            val sentiment_id = line._1.split("\t")(0)
            val created_at = line._1.split("\t")(1)
            val newScorce = line._1.split("\t")(2)

            println(line)

            //默认连接db0  可以手动修改
            jedis.select(0)
            jedis.hset(sentiment_id, created_at, newScorce + ":" + line._2)
            jedis.flushAll()
          })

          jedis.close()
        })
      })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
