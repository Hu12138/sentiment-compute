package com.shujia.util

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 作为传递类参数的类
  * spark main方法类实现接口
  *
  * @author qinxiao
  */
abstract class SparkTool {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var LOGGER: Logger = LoggerFactory.getLogger(this.getClass) //日志对象
  var hiveContext: HiveContext = _

  def main(args: Array[String]): Unit = {

//    if (args.length == 0) {
//      LOGGER.info("请传入参数 day_id")
//      return
//    }

    try {
      //加载用户自定义配置文件,集群运行起作用
      Config.loadCustomConfig(Config.getDefaultCustomConfigPath)
    } catch {
      case e: Exception =>
    }
    conf = new SparkConf()
    //设置spark appName
    conf.setAppName(this.getClass.getSimpleName.replace("$", ""))
    this.init(args)

    //本地模式不需要增加jar包
    if (!conf.get("spark.master").contains("local")) {
      //加载jar包
      loadOtherJars()
    }


    sc = new SparkContext(conf)

    if (!conf.get("spark.master").contains("local")) {
      hiveContext = new HiveContext(sc)
    }


    LOGGER.info("开始执行spark任务")
    this.run(args)
    LOGGER.info("spark任务执行完成")
  }

  /**
    * spark配置初始化方法，初始化conf对象
    */
  def init(args: Array[String])

  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    *
    * @param args
    */
  def run(args: Array[String])


  /**
    * 加载第三方jar包
    */
  def loadOtherJars(): Unit = {
    val listFiles = getLibJars()
    val jars = listFiles.filter(_.endsWith("jar"))
    //增加jar包
    conf.setJars(jars)
  }

  /**
    * 获取lib目录下所有jar包
    *
    * @return
    */
  def getLibJars(): List[String] = {
    val listFiles = new File(new File(System.getProperty("user.dir")).getParentFile, "lib")
      .listFiles
      .toList
      .map(_.getAbsolutePath)
    listFiles
  }


}