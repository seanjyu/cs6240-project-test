package project

import org.apache.spark.{SparkConf, SparkContext}

object projectTestMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("project test").setMaster("local[4]")
    val sc = new SparkContext(conf)

//    val row1 = List(1,2,3)
    val inputFile = sc.textFile(args(0))
    val mat = inputFile.flatMap(line => line.split("\n")).map(_.split(",").map(_.trim.toInt))

    val broadcastTest = sc.broadcast(mat.collect())

//    val matmul = mat.map{case (row) =>
//      val finalRow:Array[Int] = Array.fill(row.length)(0)
//      val result = broadcastTest.value.map{
//        case (col) =>
//          println("col length", col.length)
//          println("row length", row.length)
//          for (i <-0 until(col.length)) {
//            println("cur row", row(i))
//            println("cur col", col(i))
//            val mult = row(i) * col(i)
//            finalRow(i) += mult
//        }
//      }
//      finalRow
//    }
      val matmul = mat.map { row =>
        val resultRow = broadcastTest.value.map { col =>
          val products = row.zip(col).map { case (a, b) => a * b }
          products.sum
        }
        resultRow.toArray
      }
//    val matmul = broadcastTest.value.map { col =>
//      val resultRow = mat.map { row =>
//        val products = row.zip(col).map { case (a, b) => a * b }
//        products.sum
//      }
//      resultRow.toArray
//    }


//    mat.mapPartitions()

//    println("print input", mat.map(_.foreach(_.toString)))
//    mat.foreach(row => row.foreach(element => println(element.toString)))
//    println("broadcast test", matmul.collect().mkString("Array(", ", ", ")"))
//      println()
//    println("print output", matmul.collect().mkString("Array(", ", ", ")"))
    matmul.foreach(row => row.foreach(element => println("??", element.toString)))
    matmul.map(row => row.mkString(",")).saveAsTextFile(args(1))
  }

}
