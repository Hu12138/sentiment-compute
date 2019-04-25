package com.shujia.sentiment

import java.io.StringReader

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.wltea.analyzer.lucene.IKAnalyzer

import scala.collection.mutable.ListBuffer

object NaiveBayesTest {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("app")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var neg = sc.textFile("data/neg.txt")
    var pos = sc.textFile("data/pos.txt")

    val srcRDD = neg.map(_.replace("\'", "")).map((0, _))
      .union(pos.map(_.replace("\'", "")).map((1, _)))

      //使用ik分词器对文本进行分词
      .map(t => {
      val list = new ListBuffer[String]()

      val analyzer = new IKAnalyzer(false)
      val reader = new StringReader(t._2)
      val ts = analyzer.tokenStream("", reader)
      ts.reset()
      val term = ts.getAttribute(classOf[CharTermAttribute])

      while (ts.incrementToken()) {
        list += term.toString
      }
      (t._1.toString, list.mkString(" "))
    }).toDF("category", "text") //转换成df  同时执行列名

    //统计总的单词数量
    val wordSum = srcRDD.select("text")
      .rdd
      .map(_.getAs[String]("text"))
      .flatMap(_.split(" "))
      .distinct()
      .count()

    println("=============" + wordSum)


    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    //训练集，训练模型
    var trainingDF = splits(0)
    //测试模型准确率
    var testDF = splits(1)

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category", $"text", $"words")
      .take(1)
      .foreach(row => println(row.mkString("\n")))

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF()
      .setNumFeatures(wordSum.toInt) //文档库总的单词数量，x的数量，值越大，计算复杂度越高，准确率越高，有上限
      .setInputCol("words")
      .setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData
      .select($"category", $"words", $"rawFeatures")
      .take(10).foreach(row => println(row.mkString("\n")))
    //

    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    rescaledData
      .select($"category", $"features")
      .take(10)
      .foreach(row => println(row.mkString("\n")))

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, features)
    }

    println("output4：")
    trainDataRdd.
      take(1)
      .foreach(row => println(row))


    //训练模型  朴素贝叶斯
    val model = NaiveBayes.train(trainDataRdd, 1.0, "multinomial")


    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)

    //保存模型   ，保存到hdfs
    model.save(sc, "model/")


    //加载模型
    //    val inModel = NaiveBayesModel.load(sc, "model")
    //    inModel.predict(//传入构建好的x)

  }
}
