package org.dgrf.psvg

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{collect_list, min}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

class VGXYTimeSeries () extends java.io.Serializable {
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

    var minYval = inputTimeSeries.agg(min(inputTimeSeries("yval"))).first().getDouble(0)
    if (minYval < 0 ) {
      inputPositiveTimeSeries = inputTimeSeries.select(inputTimeSeries("xval"),(inputTimeSeries("yval")-minYval).alias("yval"),inputTimeSeries("id"))
    } else {
      inputPositiveTimeSeries = inputTimeSeries
    }
    var minXval = inputTimeSeries.agg(min(inputTimeSeries("xval"))).first().getDouble(0)
    if (minXval < 0 ) {
      inputPositiveTimeSeries = inputTimeSeries.select((inputTimeSeries("xval")-minXval).alias("xval"),inputTimeSeries("yval"),inputTimeSeries("id"))
    } else {
      inputPositiveTimeSeries = inputTimeSeries
    }
  }
  private def joinAndAggregateTimeSeries (): Unit = {
    val InputTimeSeriesSelf = inputTimeSeries.select(inputTimeSeries("xval").alias("xvalself"),inputTimeSeries("yval").alias("yvalself"),inputTimeSeries("id").alias("idself"))
    val inputTimeSeriesCartesian = inputTimeSeries.join(InputTimeSeriesSelf,inputTimeSeries("id")<InputTimeSeriesSelf("idself"))

    val inputTimeSeriesGroup = inputTimeSeriesCartesian.groupBy("id","xval","yval")
    inputTimeSeriesFLat = inputTimeSeriesGroup.agg(collect_list("idself"),collect_list("xvalself"),collect_list("yvalself"))
  }
  private def readSeqFileIntoDataset(): Unit = {
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("xval", DoubleType),
      StructField("yval", DoubleType)
    ))
    val sqlContext = sparkSession.sqlContext
    inputTimeSeries = sqlContext.read.schema(schema).option("delimiter",",").csv(timeSeriesWithSeqfile)
  }
  def createVisibilityGraph(): VisibilityGraph = {
    val adjschema = StructType(Seq(
      StructField("src", LongType),
      StructField("dst", LongType),
      StructField("nodedistance",DoubleType)
    ))
    val vgAdjacencyEncoder = RowEncoder(adjschema)

    val vgAdjacencyEdges = inputTimeSeriesFLat.flatMap(row => calculateVisibilityEdges(row))(vgAdjacencyEncoder)

    val vg = new VisibilityGraph(sparkSession,vgAdjacencyEdges)
    vg
  }
  private def calculateVisibilityEdges(row: Row):IndexedSeq[Row] = {
    val currentNode = (row.getLong(0) , row.getDouble(1),row.getDouble(2))

    val nodesToCompareIndex: mutable.WrappedArray[Long] = row.getAs[mutable.WrappedArray[Long]](3)
    val nodesToCompareX: mutable.WrappedArray[Double] = row.getAs[mutable.WrappedArray[Double]](4)
    val nodesToCompareY: mutable.WrappedArray[Double] = row.getAs[mutable.WrappedArray[Double]](5)
    val nodesToCompareTuple = nodesToCompareIndex zip nodesToCompareX zip nodesToCompareY map {
      case ((i,x), y) => (i,x,y)
    }

    val nodesToCompare = nodesToCompareTuple.sortBy(m => m._1)
    val visibleNodesAll = nodesToCompare.map(row=>checkVisibilityForANodeWithRest(nodesToCompare,row,currentNode))

    val visibleNodes = visibleNodesAll.filter(x=> x.getLong(0)!= -1)

    visibleNodes
  }

  def checkVisibilityForANodeWithRest (nodesToCompare:mutable.WrappedArray[(Long,Double,Double)],nodeToCompare:(Long,Double,Double),currentNode:(Long,Double,Double)): Row = {

    val nodeToCompareIndex = nodeToCompare._1
    val nodeToCompareX = nodeToCompare._2

    val currentNodeIndex = currentNode._1
    val currentNodeX = currentNode._2
    var sliceIndex  = nodeToCompareIndex-currentNodeIndex-1
    if (sliceIndex == 0) {
      sliceIndex =1
    }

    val nodesToCompareIntermediate = nodesToCompare.slice(0,sliceIndex.toInt)

    val visCheckMap = nodesToCompareIntermediate.map(nodeIntermediate=>checkVisibilityBetweenTwo(nodeIntermediate,currentNode,nodeToCompare))
    val isVisible = !visCheckMap.contains(true)
    if (isVisible) {
      val nodeDistance  = nodeToCompareX - currentNodeX
      Row(currentNodeIndex,nodeToCompareIndex,nodeDistance.toDouble)
    } else {
      Row(-1.toLong, -1.toLong,-1.toDouble)
    }
  }

  def checkVisibilityBetweenTwo (nodeIntermediate:(Long,Double,Double),currentNode:(Long,Double,Double),nodeToCompare:(Long,Double,Double)): Boolean = {
    val nodeToComapreIndex = nodeToCompare._1
    val nodeToCompareX = nodeToCompare._2
    val nodeToCompareY = nodeToCompare._3

    val currentNodeIndex = currentNode._1
    val currentNodeX = currentNode._2
    val currentNodeY = currentNode._3

    val nodeIntermediateIndex = nodeIntermediate._1
    val nodeIntermediateX = nodeIntermediate._2
    val nodeIntermediateY = nodeIntermediate._3
    if ((currentNodeIndex+1) == nodeToComapreIndex) {
      false
    } else {
      val baseRatio = (nodeIntermediateX - currentNodeX) / (nodeToCompareX - currentNodeX)
      val inBetweenHeight = (baseRatio * (nodeToCompareY - currentNodeY)) + currentNodeY

      if (nodeIntermediateY >= inBetweenHeight) {
        true
      } else {
        false
      }
    }
  }
  /*private def calcVG(row: Row):IndexedSeq[Row] = {
    val currentNodeIndex = row.getLong(0)
    val currentNodeX = row.getDouble(1)
    val currentNodeY = row.getDouble(2)
    val nodesToCompareIndex: mutable.WrappedArray[Long] = row.getAs[mutable.WrappedArray[Long]](3)
    val nodesToCompareX: mutable.WrappedArray[Double] = row.getAs[mutable.WrappedArray[Double]](4)
    val nodesToCompareY: mutable.WrappedArray[Double] = row.getAs[mutable.WrappedArray[Double]](5)
    val nodesToCompareTuple = (nodesToCompareIndex , nodesToCompareX ,nodesToCompareY).zipped.toList
    val nodesToCompare = nodesToCompareTuple.sortBy(m => m._1)
    for (nodeGap <- nodesToCompare.seq.indices) yield {
      val nodeToComapreIndex = nodesToCompare(nodeGap)._1
      val nodeToCompareX = nodesToCompare(nodeGap)._2
      val nodeToCompareY = nodesToCompare(nodeGap)._3
      if (nodeGap == 0) {
        val nodeDistance  = nodeToCompareX - currentNodeX
        Row(currentNodeIndex, nodeToComapreIndex,nodeDistance)
        //Row(-1.toDouble, -1.toDouble)
      } else {
        var visIdentified = true
        var inBetweenNodeCounter = 0
        while ((inBetweenNodeCounter < nodeGap) && visIdentified) {
          //val inBetweenNodeIndex = nodesToCompare(inBetweenNodeCounter)._1
          val inBetweenNodeX = nodesToCompare(inBetweenNodeCounter)._2
          val inBetweenNodeY = nodesToCompare(inBetweenNodeCounter)._3

          val baseRatio = (inBetweenNodeX - currentNodeX) / (nodeToCompareX - currentNodeX)
          val inBetweenHeight = (baseRatio * (nodeToCompareY - currentNodeY)) + currentNodeY

          if (inBetweenNodeY >= inBetweenHeight) {

            visIdentified = false
          }
          inBetweenNodeCounter = inBetweenNodeCounter + 1
        }
        if (visIdentified) {
          val nodeDistance  = nodeToCompareX - currentNodeX
          Row(currentNodeIndex, nodeToComapreIndex,nodeDistance)
        } else {
          Row(-1.toLong, -1.toLong,-1.toDouble)
        }
      }
    }

  }*/

}

