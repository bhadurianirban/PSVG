package org.dgrf.psvg


import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.graphframes._

import scala.math.log

class VisibilityGraph()  extends java.io.Serializable {

  private var vgGraph:GraphFrame = _
  private var vgAdjacencyEdges:Dataset[Row] = _
  private var sparkSession: SparkSession = _
  def this (sparkSession: SparkSession,vgAdjacencyEdges:Dataset[Row]) {
    this()
    this.sparkSession = sparkSession
    this.vgGraph = GraphFrame.fromEdges(vgAdjacencyEdges)
    this.vgAdjacencyEdges = vgAdjacencyEdges
    //val edges: RDD[Edge[Double]] = vgAdjacencyEdges.rdd.map (edgeLine => {
    //  Edge (edgeLine.getLong(0),edgeLine.getLong(1),edgeLine.getDouble(2))
    //})
    //this.vgGraph = Graph.fromEdges(edges,"1")
  }
  def saveVisibilityGraphAdjacencyList(outFileName:String,hdfsMode:Boolean): Unit = {
    PSVGUtil.writeDataSetToFile(vgAdjacencyEdges,outFileName,hdfsMode)
  }
/*  def getVisibilityGraph():Graph[String,Double] = {
    vgGraph
  }*/


  def calculateDegreeDistribution(psvgParams: PsvgParams = PsvgParams(15, 0.85, includeIntercept = true, 0.7, 2)): VisibityDegreeDistribution = {
  //def calculateDegreeDistribution(psvgParams: PsvgParams = new PsvgParams(15,0.85,true,0.7,2)): Unit = {

    println(psvgParams.degreeDistDataPartFromStart)
    println(psvgParams.includeIntercept)
    println(psvgParams.logBase)
    println(psvgParams.degreeDistStart)

    val noOfNode = vgGraph.vertices.count
    val degrees = vgGraph.degrees

    val degreeCount = degrees.groupBy("degree").count()
    val schema = StructType(Seq(
      StructField("k", IntegerType),
      StructField("countOfk", LongType),
      StructField("Pk", DoubleType),
      StructField("logInverseK", DoubleType),
      StructField("logPk", DoubleType)
    ))
    val KpKEncoder = RowEncoder(schema)

    val degreeDistribution = degreeCount.map(m=> {
      val k = m.getInt(0)
      val countOfk = m.getLong(1)
      val Pk = m.getLong(1)/noOfNode.toDouble
      val logK = log(1/k.toDouble)/log(psvgParams.logBase)
      val logPk = log(Pk)/log(psvgParams.logBase)
      Row(k,countOfk,Pk,logK,logPk)
    })(KpKEncoder)
    degreeDistribution.persist
    val vdd = new VisibityDegreeDistribution(sparkSession,degreeDistribution,psvgParams.degreeDistStart,psvgParams.degreeDistDataPartFromStart,psvgParams.includeIntercept,psvgParams.rejectCut)
    vdd
  }

}
