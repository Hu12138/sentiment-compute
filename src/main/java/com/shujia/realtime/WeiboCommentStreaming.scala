package com.shujia.realtime

import com.shujia.util.SparkTool
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

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

    val DS = KafkaUtils.createStream(
      ssc,
      "node1:2181,node2:2181,node3:2181",
      "comment1",
      Map("WeiBoCommentTopic" -> 4)
    )


    DS
      .map(_._2)
      .transform(rdd => {
        val DF = sqlContext.read.json(rdd)
        DF.rdd
      })
      .map(row => {
        val article_id = row.getAs[String]("article_id")
        val score = row.getAs[String]("score")
        article_id + "\t" + score
      }).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
