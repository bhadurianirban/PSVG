package org.dgrf.psvg

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{collect_list, min}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

class VGUniformTimeSeries extends java.io.Serializable {
  private var sparkSession:SparkSession = _
  private var timeSeriesWithSeqfile:String = _
  private var inputTimeSeriesFLat: Dataset[Row] = _
  private var inputTimeSeries: Dataset[Row] = _
  private var inputPositiveTimeSeries: Dataset[Row] = _

  def this (sparkSession:SparkSession,timeSeriesWithSeqfile:String)  {
    this()
    this.sparkSession = sparkSession
    this.timeSeriesWithSeqfile = timeSeriesWithSeqfile
    readSeqFileIntoDataset()
    moveTimeSeriesToPositivePlane()
    joinAndAggregateTimeSeries()
  }



  private def moveTimeSeriesToPositivePlane (): Unit = {

    val minYval = inputTimeSeries.agg(min(inputTimeSeries("yval"))).first().getDouble(0)
    if (minYval < 0 ) {
      inputPositiveTimeSeries = inputTimeSeries.select((inputTimeSeries("yval")-minYval).alias("yval"),inputTimeSeries("id"))
    } else {
      inputPositiveTimeSeries = inputTimeSeries
    }

  }
  private def joinAndAggregateTimeSeries (): Unit = {
    val InputTimeSeriesSelf = inputPositiveTimeSeries.select(inputPositiveTimeSeries("yval").alias("yvalself"),inputPositiveTimeSeries("id").alias("idself"))
    InputTimeSeriesSelf.persist()
    inputPositiveTimeSeries.persist()
    val inputTimeSeriesCartesian = inputPositiveTimeSeries.join(InputTimeSeriesSelf,inputPositiveTimeSeries("id")<InputTimeSeriesSelf("idself"))

    val inputTimeSeriesGroup = inputTimeSeriesCartesian.groupBy("id","yval")
    inputTimeSeriesFLat = inputTimeSeriesGroup.agg(collect_list("idself"),collect_list("yvalself"))
    inputTimeSeriesFLat.persist()

  }
  private def readSeqFileIntoDataset(): Unit = {
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("yval", DoubleType)
    ))
    val sqlContext = sparkSession.sqlContext
    println(timeSeriesWithSeqfile)
    inputTimeSeries = sqlContext.read.schema(schema).option("delimiter",",").csv("file://"+timeSeriesWithSeqfile)
  }
  def createVisibilityGraph(): VisibilityGraph = {
    val vgAdjacencyEdges = createVGAdjEdges()
    //vgAdjacencyEdges.show(5)
    val vg = new VisibilityGraph(sparkSession,vgAdjacencyEdges)
    vg
  }
  private def createVGAdjEdges (): Dataset[Row] = {
    val adjschema = StructType(Seq(
      StructField("src", LongType),
      StructField("dst", LongType),
      StructField("nodedistance",DoubleType)
    ))
    val vgAdjacencyEncoder = RowEncoder(adjschema)
    //inputTimeSeriesFLat.take(5).foreach(row=>calcVG(row))

    val vgAdjacencyEdges = inputTimeSeriesFLat.flatMap(row => calculateVisibilityEdges(row))(vgAdjacencyEncoder)
    vgAdjacencyEdges
  }

  private def calculateVisibilityEdges(row: Row):IndexedSeq[Row] = {
    val currentNode = (row.getLong(0) , row.getDouble(1))
    val nodesToCompareIndex: mutable.WrappedArray[Long] = row.getAs[mutable.WrappedArray[Long]](2)

    val nodesToCompareY: mutable.WrappedArray[Double] = row.getAs[mutable.WrappedArray[Double]](3)
    val nodesToCompareTuple = nodesToCompareIndex zip nodesToCompareY
    val nodesToCompare = nodesToCompareTuple.sortBy(m => m._1)
    val visibleNodesAll = nodesToCompare.map(row=>checkVisibilityForANodeWithRest(nodesToCompare,row,currentNode))

    val visibleNodes = visibleNodesAll.filter(x=> x.getLong(0)!= -1)

    visibleNodes
  }

  def checkVisibilityForANodeWithRest (nodesToCompare:mutable.WrappedArray[(Long,Double)],nodeToCompare:(Long,Double),currentNode:(Long,Double)): Row = {

    val nodeToCompareIndex = nodeToCompare._1

    val currentNodeIndex = currentNode._1
    var sliceIndex  = nodeToCompareIndex-currentNodeIndex-1
    if (sliceIndex == 0) {
      sliceIndex =1
    }

    val nodesToCompareIntermediate = nodesToCompare.slice(0,sliceIndex.toInt)

    val visCheckMap = nodesToCompareIntermediate.map(nodeIntermediate=>checkVisibilityBetweenTwo(nodeIntermediate,currentNode,nodeToCompare))
    val isVisible = !visCheckMap.contains(true)
    if (isVisible) {
      val nodeDistance  = nodeToCompareIndex - currentNodeIndex
      Row(currentNodeIndex,nodeToCompareIndex,nodeDistance.toDouble)
    } else {
      Row(-1.toLong, -1.toLong,-1.toDouble)
    }
  }

  def checkVisibilityBetweenTwo (nodeIntermediate:(Long,Double),currentNode:(Long,Double),nodeToCompare:(Long,Double)): Boolean = {
    val nodeToComapreIndex = nodeToCompare._1
    val nodeToCompareY = nodeToCompare._2
    val currentNodeIndex = currentNode._1
    val currentNodeY = currentNode._2
    val nodeIntermediateIndex = nodeIntermediate._1
    val nodeIntermediateY = nodeIntermediate._2
    if ((currentNodeIndex+1) == nodeToComapreIndex) {
       false
    } else {
      val baseRatio = (nodeIntermediateIndex.toDouble - currentNodeIndex) / (nodeToComapreIndex - currentNodeIndex)
      val inBetweenHeight = (baseRatio * (nodeToCompareY - currentNodeY)) + currentNodeY

      if (nodeIntermediateY >= inBetweenHeight) {
        true
      } else {
        false
      }
    }
  }
}
  //println("gheu")
  //visMatrix.foreach(m=>println(m.getLong(0)+" "+m.getLong(1)))
  /*for (nodeGap <- nodesToCompare.seq.indices) yield {
      val nodeToComapreIndex = nodesToCompare(nodeGap)._1
      val nodeToCompareY = nodesToCompare(nodeGap)._2

      if (nodeGap == 0) {
        val nodeDistance  = nodeToComapreIndex - currentNodeIndex

        Row(currentNodeIndex, nodeToComapreIndex,nodeDistance.toDouble)
        //Row(-1.toDouble, -1.toDouble)
      } else {
        var visIdentified = true
        var inBetweenNodeCounter = 0
        while ((inBetweenNodeCounter < nodeGap) && visIdentified) {
          //val inBetweenNodeIndex = nodesToCompare(inBetweenNodeCounter)._1
          val inBetweenNodeIndex = nodesToCompare(inBetweenNodeCounter)._1
          val inBetweenNodeY = nodesToCompare(inBetweenNodeCounter)._2

          val baseRatio = (inBetweenNodeIndex.toDouble - currentNodeIndex) / (nodeToComapreIndex - currentNodeIndex)
          val inBetweenHeight = (baseRatio * (nodeToCompareY - currentNodeY)) + currentNodeY

          if (inBetweenNodeY >= inBetweenHeight) {

            visIdentified = false
          }
          inBetweenNodeCounter = inBetweenNodeCounter + 1
        }
        if (visIdentified) {
          val nodeDistance  = nodeToComapreIndex - currentNodeIndex
          Row(currentNodeIndex, nodeToComapreIndex,nodeDistance.toDouble)
        } else {
          Row(-1.toLong, -1.toLong,-1.toDouble)
        }
      }
    }*/
  /*def createVisibilityGraph(): VisibilityGraph = {
  val adjschema = StructType(Seq(
    StructField("startNode", LongType),
    StructField("endNode", LongType),
    StructField("nodedistance",DoubleType)
  ))
  val vgAdjacencyEncoder = RowEncoder(adjschema)

  val vgAdjacencyEdges = inputTimeSeriesFLat.flatMap(row => calcVG(row))(vgAdjacencyEncoder).filter(x=> x.getLong(0)!= -1)
  val vg = new VisibilityGraph(sparkSession,vgAdjacencyEdges)
  vg
}*/

