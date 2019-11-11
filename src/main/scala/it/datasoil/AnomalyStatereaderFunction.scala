package it.datasoil

import com.tdunning.math.stats.AVLTreeDigest
import it.datasoil.anomalydetector.{AnomalyStepConfiguration, CrossCorrelDetector, Detector, UniSeasDetector}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector
import org.joda.time.{DateTime, DateTimeZone}
import org.mongodb.scala.Document
import org.mongodb.scala.bson.ObjectId
import org.slf4j.LoggerFactory

class AnomalyStateReaderFunction extends KeyedStateReaderFunction[String, Document] {

  @transient lazy val LOG = LoggerFactory.getLogger(classOf[AnomalyStateReaderFunction])

  var pipestate: MapState[String,String] = null
  var transformersState: MapState[String, TransformStepConfiguration] = null
  var mapToDetector: MapState[String, Map[String, Detector]] = null
  var detectorsConfigs: MapState[String,AnomalyStepConfiguration] = null

  override def open(parameters: Configuration): Unit = {
    val pipeStatusDescriptor = new MapStateDescriptor[String, String]("pipeline-status", classOf[String], classOf[String])
    val tstatedesc =  new MapStateDescriptor[String, TransformStepConfiguration]("transformers", classOf[String], classOf[TransformStepConfiguration])
    pipestate = getRuntimeContext.getMapState(pipeStatusDescriptor)
    transformersState = getRuntimeContext.getMapState(tstatedesc)
    mapToDetector = getRuntimeContext
      .getMapState(new MapStateDescriptor[String,Map[String, Detector]]("mapToDetector", classOf[String],classOf[Map[String,Detector]]))
    detectorsConfigs = getRuntimeContext
      .getMapState(new MapStateDescriptor[String, AnomalyStepConfiguration]("detectorsConfigs",classOf[String],classOf[AnomalyStepConfiguration]))
  }

  override def readKey(k: String, context: KeyedStateReaderFunction.Context, out: Collector[Document]): Unit = {
    // k is the pipeID
    val mapKeys = mapToDetector.keys().iterator()
    LOG.info(s"++++++++++ PipeID=$k")
    while (mapKeys.hasNext){
      val currentKey = mapKeys.next() // detectorID
      LOG.info(s">>>>>>>>>>>> Inspecting Key $currentKey")
      val innerMap = mapToDetector.get(currentKey)
      out.collect(AnomalyStateReaderFunction.parseToDocument(k, currentKey, innerMap))
    }
  }
}
// the following companion object defines some static methods used to form the bson documents summarizing the state
object AnomalyStateReaderFunction {

  @transient lazy val LOG = LoggerFactory.getLogger(classOf[AnomalyStateReaderFunction])

  def parseToDocument(pipeID: String, detectorID: String, mapToDetectors: Map[String, Detector]): Document = {

    val nowAsString = DateTime.now().toDateTime(DateTimeZone.UTC).toString
    val inner = mapToDetectors.toSeq.map(extractDetectorInfo(_)).foldLeft(List[Document]())(_ :+ _)
    return Document("pipe_id" -> pipeID, "ad_id" -> detectorID, "ts" -> nowAsString, "ad_data" -> inner)
  }

  def extractDetectorInfo(value: (String, Detector)): Document = {
    val currentDetector = value._2
    val id = value._1
    currentDetector match {

      case d: UniSeasDetector => {

        val thresholdList = d.mapToDigest.toSeq.map(entry => Document(
          "day" -> entry._1._2.toString,
          "hour" -> entry._1._3,
          "nobs" -> entry._2.size(),
          "percs_thrs" -> extractQuantilesThresholdsFromTree(d.configParams.anomalyPercent, entry._2)
        )).foldLeft(List[Document]())(_ :+ _)

        return Document("aid" -> new ObjectId(id),
          "ad_type" -> d.configParams.detectorType,
          "perc" -> d.configParams.anomalyPercent,
          "percs_thrs" -> thresholdList
        )

      }

      case d: CrossCorrelDetector => {

        return Document(
          "aid" -> new ObjectId(id),
          "ad_type" -> d.configParams.detectorType,
          "perc" -> d.configParams.anomalyPercent,
          "wsz" -> d.configParams.windowSizeH,
          "nobs" -> d.tree.size(),
          "percs_thrs" -> extractQuantilesThresholdsFromTree(d.configParams.anomalyPercent, d.tree),
          "quantiles" -> (0.0 to 1 by 0.1).map(d.tree.quantile(_)).foldLeft(List[Double]())(_ :+ _)
        )

      }

    }
    return Document("error" -> "not recognised detector for this asset") // fallback case
  }

  def extractQuantilesThresholdsFromTree(anomalyPercent: Double, tree: AVLTreeDigest): List[(String, Double)] = {
    val infPercent = anomalyPercent / (2 *100)
    val supPercent = 1 - anomalyPercent / (2 *100)
    val supKey = (supPercent * 100).toString.replace(".","_")//.concat("%")
    val infKey = (infPercent * 100).toString.replace(".","_")//.concat("%")
    List((supKey, tree.quantile(supPercent)), (infKey, tree.quantile(infPercent)))
  }

}