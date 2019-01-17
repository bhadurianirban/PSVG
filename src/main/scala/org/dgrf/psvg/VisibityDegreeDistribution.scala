package org.dgrf.psvg

import java.io.FileWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class VisibityDegreeDistribution {
  private var degreeDistribution:Dataset[Row] = _
  private var sparkSession: SparkSession = _
  var psvgResults:PSVGResults = _


  def this (sparkSession: SparkSession,degreeDistribution:Dataset[Row],degreeDistStart:Int=15,degreeDistDataPartFromStart:Double=0.85,includeIntercept:Boolean=true,rejectCut:Double=0.7) = {
    this()
    this.degreeDistribution = degreeDistribution
    this.sparkSession = sparkSession
    calculateFractalDimension(degreeDistStart,degreeDistDataPartFromStart,includeIntercept,rejectCut)
  }

  private def calculateFractalDimension(degreeDistStart:Int,degreeDistDataPartFromStart:Double,includeIntercept:Boolean,rejectCut:Double): Unit = {

    case class DegreeDist(k:Int,countOfk:Long,Pk:Double,logK:Double,logPk:Double)
    val degreeList = degreeDistribution.select("k","countOfk","Pk","logInverseK","logPk").orderBy("k").collect.toList
    val deg = degreeList.map(row => {
      DegreeDist(row.getInt(0), row.getLong(1), row.getDouble(2), row.getDouble(3), row.getDouble(4))
    })
    val degreeDistEnd = (deg.size*degreeDistDataPartFromStart).toInt +1

    val relevantDegreeDist = deg.slice(degreeDistStart,degreeDistEnd)


    val regset = new SimpleRegression(includeIntercept)
    relevantDegreeDist.foreach(m=> regset.addData(m.logK,m.logPk))

    psvgResults = PSVGResults(regset.getSlope, regset.getIntercept, regset.getRSquare, regset.getSlopeStdErr, regset.getInterceptStdErr)

  }

  def savePSVGResults(outFileName:String,hdfsMode:Boolean): PSVGResults = {
    PSVGUtil.writeDataSetToFile(degreeDistribution,outFileName,hdfsMode)
    val objMapper = new ObjectMapper()
    objMapper.registerModule(DefaultScalaModule)

    val psvgResultsJSONString = objMapper.writeValueAsString(psvgResults)
    val psvgJSONFileName = "/tmp/"+outFileName+".json"
    val psvgJSONFile = new FileWriter(psvgJSONFileName)
    psvgJSONFile.write(psvgResultsJSONString)
    psvgJSONFile.close()
    println(psvgResultsJSONString)
    psvgResults
  }
}
