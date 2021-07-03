package it.unict.gigig

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

import scala.reflect.ClassTag
import javax.xml.validation.SchemaFactoryLoader

object SimRankClassic {

  def printMatrix(M: Array[Array[Float]], dim: Int) = {
    for(i <- 0 to dim - 1; j <- 0 until dim) {
      print(i, j)
      println("=" + M(i)(j))
    }
  }

  def printError = {
    println("\nUsage: spark-submit --class it.unict.gigig.SimRank --num-executors 2 --master spark://192.168.1.21:7077 target/simrank-1.0-SNAPSHOT.jar <inputFile> <decay> <iters>\n")
    println("- inputFile: name of the file.")
    println("- decay: importance value.")
    println("- iters: number of iteration.\n")
    System.exit(-1)
  }

  def get_adjacency_matrix[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], dim: Int): Array[Array[Float]] = {
    val adjMatrix = Array.ofDim[Float](dim, dim) 
    val verticesWithSuccessors: VertexRDD[Array[VertexId]] = graph.ops.collectNeighborIds(EdgeDirection.Out)
    val successorGraph = Graph(verticesWithSuccessors, graph.edges)
    val adjList = successorGraph.vertices
    adjList.collect().map { adj =>
      val i = adj._1.toInt
      adj._2.map { j =>
        adjMatrix(i)(j.toInt) = 1
      }
    }
    adjMatrix
  }

  def get_similarity_matrix(dim: Int): Array[Array[Float]] = {
    var S = Array.ofDim[Float](dim, dim)
    for(i <- 0 to dim - 1) {
      S(i)(i) = 1
    }
    S
  }

  def calculate_similarity_scores[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], similarity_matrix: Array[Array[Float]], dim: Int, adjMatrixT: Array[Array[Float]], iters: Int, decay: Float): Array[Array[Float]] = {
    val current_matrix = similarity_matrix
    for(c <- 0 to iters - 1) {
      val old_matrix = current_matrix.clone()
      graph.vertices.cartesian(graph.vertices).distinct.collect().map { case ((vid1, _), (vid2, _)) =>
        val i = vid1.toInt
        val j = vid2.toInt
        var k = 0
        val incoming_a = adjMatrixT(i).map{ v => 
          k = k + 1
          if(v > 0) {
            k - 1
          }
          else {
            -1
          }
        }.toArray
        k = 0
        val incoming_b = adjMatrixT(j).map{ v => 
          k = k + 1
          if(v > 0) {
            k - 1
          }
          else {
            -1
          }
        }.toArray
        val len_a = adjMatrixT(i).map( v => if(v > 0) 1 else 0 ).sum
        val len_b = adjMatrixT(j).map( v => if(v > 0) 1 else 0 ).sum
        if(len_a == 0 || len_b == 0) {
          current_matrix(i)(j) = 0
        }
        else {
          var total_score:Float = 0
          for(a <- incoming_a) {
            if(a != -1) {
              for(b <- incoming_b) {
                if(b != -1) {
                  total_score = total_score + old_matrix(incoming_a.indexOf(a))(incoming_b.indexOf(b))
                }
              }
            }
          }
          //println("len_a=" + len_a + " len_b=" + len_b + " i=" + i + " j=" + j)
          val score = (decay / (len_a * len_b)) * total_score
          current_matrix(i)(j) = score
          current_matrix(j)(i) = score
        }
      }
      for(c <- 0 to dim - 1) {
        current_matrix(c)(c) = 1
      }
    }
    current_matrix
  }

  def compute_simRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], iters: Int, decay: Float) = {
    val vertices = graph.vertices
    val dim = vertices.count().toInt
    val adjMatrix = get_adjacency_matrix(graph, dim)
    //printMatrix(adjMatrix, dim)
    //println()
    val similarity_matrix = get_similarity_matrix(dim)
    val final_matrix = calculate_similarity_scores(graph, similarity_matrix, dim, adjMatrix.transpose, iters, decay)
    printMatrix(final_matrix, dim)
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
    var decay = 0.8f
    var iters = 10

    try {
      inputFile = args(0).split("/").last
      decay = args(1).toFloat
      iters = args(2).toInt
    } catch {
        case _: Exception =>{
          printError
        }
    }

    val conf = new SparkConf().setAppName("SimRankClassic")
    val sc = new SparkContext(conf)

    val inputGraph = GraphLoader.edgeListFile(sc, inputFile)
    
    compute_simRank(inputGraph, iters, decay)
    
    sc.stop()
  }
}
