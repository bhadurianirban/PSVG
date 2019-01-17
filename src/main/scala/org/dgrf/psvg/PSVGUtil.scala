package org.dgrf.psvg

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import scala.io.Source

object PSVGUtil {
  def readParametersFromJson (paramFilePath:String): PsvgParams = {
    val psvgParmanJSON = Source.fromFile(paramFilePath).mkString

    val objMapper = new ObjectMapper()
    objMapper.registerModule(DefaultScalaModule)

    val psvgParamObj = objMapper.readValue(psvgParmanJSON,classOf[PsvgParams])
    psvgParamObj
  }
  def writeDataSetToFile(datasetToWrite:Dataset[Row],outputFileName:String,hdfsMode:Boolean): Unit = {
    if (hdfsMode) {
      val outFileName = "hdfs://localhost:9000/"+outputFileName
      datasetToWrite.write.mode(SaveMode.Overwrite).csv(outFileName)
    } else {
      datasetToWrite.write.mode(SaveMode.Overwrite).csv(outputFileName)
    }
  }
}
