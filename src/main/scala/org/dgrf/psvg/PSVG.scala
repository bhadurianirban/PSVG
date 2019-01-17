package org.dgrf.psvg

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.SparkSession

import scala.io.Source

class PSVG(sparkSession:SparkSession) {
  private val timeSeriesWithSeqfile = "/tmp/addedSeqFile.csv"

  def readXYTimeSeries(XYTimeSeriesFile:String): VGXYTimeSeries = {
    readXYDataFileAndAddSequence(XYTimeSeriesFile)

    val vgcalc = new VGXYTimeSeries(sparkSession,timeSeriesWithSeqfile)
    vgcalc
  }
  //lkdfjg
  def readUniformTimeSeries(UniformTimeSeriesFile:String): VGUniformTimeSeries = {
    readUniformDataFileAndAddSequence(UniformTimeSeriesFile)

    val vgcalc = new VGUniformTimeSeries(sparkSession,timeSeriesWithSeqfile)
    vgcalc
  }
  private def readXYDataFileAndAddSequence(XYTimeSeriesFile:String): Unit = {
    val bufferedSource = Source.fromFile(XYTimeSeriesFile)
    val outFileWriter = new File(timeSeriesWithSeqfile)
    val bw = new BufferedWriter(new FileWriter(outFileWriter))
    var lineCounter:Long = 0
    for (line <- bufferedSource.getLines) {
      val XYData = line.split(",")

      //println (lineCounter+" n "+gheu(0)+" g "+gheu(1))
      bw.write(lineCounter+","+XYData(0)+","+XYData(1)+"\n")
      lineCounter = lineCounter+1
    }
    bw.close()
    bufferedSource.close()
  }
  private def readUniformDataFileAndAddSequence(UniformTimeSeriesFile:String): Unit = {
    val bufferedSource = Source.fromFile(UniformTimeSeriesFile)
    val outFileWriter = new File(timeSeriesWithSeqfile)
    val bw = new BufferedWriter(new FileWriter(outFileWriter))
    var lineCounter:Long = 0
    for (line <- bufferedSource.getLines) {


      //println (lineCounter+" n "+gheu(0)+" g "+gheu(1))

      bw.write(lineCounter+","+line+"\n")
      lineCounter = lineCounter+1
    }
    bw.close()
    bufferedSource.close()
    println("Written temp file "+timeSeriesWithSeqfile)
  }

}
