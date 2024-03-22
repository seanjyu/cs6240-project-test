package project

import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object projectTest2Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("project test2").setMaster("local[4]")
    val sc = new SparkContext(conf)
//    val numFeatures = 3

//    val mat: RDD[Int] = sc.emptyRDD[Int]
//    val matTranspose: RDD[Int] = sc.emptyRDD[Int]


    val inputFile = sc.textFile(args(0))
    val mat = inputFile.flatMap {
      case (line) =>
        line.split("\n").map(_.split(",").map(_.trim.toInt))
    }
    val numFeatures = mat.first().length

    val matMul = mat.flatMap {
      case (line) =>
//        println("line", line.mkString("Array(", ", ", ")"))
        line.zipWithIndex.flatMap {
          case (element, index) =>
            println("ele: ", element)
            println("ind: ", index)
            (0 until numFeatures - index).map { i =>
//              println("i", i)
//              println("j", element)
//              println("ixj", line(i) * line(index))
              ((index, numFeatures - i - 1), line(numFeatures - i - 1) * element)
            }
        }
    }.reduceByKey((x, y) => x + y)
    println("matmul", matMul.collect().mkString("Array(", ", ", ")"))
    matMul.saveAsTextFile(args(1))
    println("project test 2 works")
  }
}
