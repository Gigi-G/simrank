package it.unict.gigig

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import scala.math.sqrt

import scala.reflect.ClassTag

object SimRankMatrix {

  def dotProduct(vector: Array[Float], matrix: Array[Array[Float]]): Array[Float] = { 
    (0 to (matrix(0).size - 1)).toArray.map( colIdx => { 
        val colVec: Array[Float] = matrix.map( rowVec => rowVec(colIdx) ) 
        val elemWiseProd: Array[Float] = (vector zip colVec).map( entryTuple => entryTuple._1 * entryTuple._2 ) 
        elemWiseProd.sum 
    })
  }

  def simRank(W: Array[Array[Float]], k: Int, dim: Int, sc: SparkContext) = {
    var S = Array.ofDim[Float](dim, dim)
    for(i <- 0 to dim - 1) {
      S(i)(i) = 1
    }
    for(i <- 0 to k - 1) {
      val A = sc.parallelize(W)
      val B = sc.broadcast(S)
      S = A.map( row => dotProduct(row, B.value) ).collect
      val C = sc.parallelize(W)
      val D = sc.broadcast(S.transpose)
      S = C.map( row => dotProduct(row, D.value) ).collect
      for(j <- 0 to dim - 1) {
        S(j)(j) = 1
      }
    }

    val save = S.flatMap { row => 
      val i = S.indexOf(row)
      val r = row
      row.map { v =>
        val j = r.indexOf(v)
        (i, j, v)
      }
    }

    sc.parallelize(save).saveAsTextFile("hdfs://192.168.1.21:9000/simrank/output")
    println("The result has been saved on hadoop.")
    //printMatrix(S, dim)
  }

  def printMatrix(M: Array[Array[Float]], dim: Int) = {
    for(i <- 0 to dim - 1; j <- 0 until dim) {
      print(i, j)
      println("=" + M(i)(j))
    }
  }

  def minMax(a: Array[Float]) : (Float, Float) = {
    a.foldLeft((a(0), a(0)))
    { case ((min, max), e) => (math.min(min, e), math.max(max, e))}
  }

  def printError = {
    println("\nUsage: spark-submit --class it.unict.gigig.SimRank --num-executors 2 --master spark://192.168.1.21:7077 target/simrank-1.0-SNAPSHOT.jar <inputFile> <decay> <iters>\n")
    println("- inputFile: name of the file.")
    println("- decay: importance value.")
    println("- iters: number of iteration.\n")
    System.exit(-1)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("warn").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("info").setLevel(Level.OFF)

    if(args.length != 3) {
      printError
    }

    var inputFile = "prova.txt"
    var C = 0.8f
    var iters = 10

    try {
      inputFile = args(0).split("/").last
      C = args(1).toFloat
      iters = args(2).toInt
    } catch {
        case _: Exception =>{
          printError
        }
    }

    val conf = new SparkConf().setAppName("learn").setMaster("local")
    val sc = new SparkContext(conf)

    val inputGraph = GraphLoader.edgeListFile(sc, "hdfs://192.168.1.21:9000/simrank/input/" + inputFile)
    //val inputGraph = GraphLoader.edgeListFile(sc, inputFile)
    val vertices = inputGraph.vertices
    val dim = vertices.count().toInt
    val adjMatrix = Array.ofDim[Float](dim, dim) 
    val verticesWithSuccessors: VertexRDD[Array[VertexId]] = inputGraph.ops.collectNeighborIds(EdgeDirection.In)
    val successorGraph = Graph(verticesWithSuccessors, inputGraph.edges)
    val adjList = successorGraph.vertices

    adjList.collect().map { adj =>
      val i = adj._1.toInt
      adj._2.map { j =>
        adjMatrix(i)(j.toInt) = 1
      }
    }

    val weight = adjMatrix.map( row => row.sum ).toArray

    adjList.collect().map { adj =>
      val i = adj._1.toInt
      adj._2.map { j =>
        adjMatrix(i)(j.toInt) = adjMatrix(i)(j.toInt) / weight(i) * sqrt(C).toFloat
      }
    }

    simRank(W = adjMatrix, dim = dim, k = iters, sc = sc)
    
    sc.stop()
  }
}
