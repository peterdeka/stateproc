package it.datasoil.anomalydetector

import java.time.{DayOfWeek, ZonedDateTime}
import java.util.Calendar

import com.tdunning.math.stats.AVLTreeDigest
import it.datasoil.BumblebeeOutput
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsError, JsPath, JsSuccess}


trait Detector {
  def isAnomaly(bbOut: BumblebeeOutput): Option[Anomaly]
  def updateCfg(config: AnomalyStepConfiguration): Unit
}

object Detector {
  def createDetector( cfg:AnomalyStepConfiguration): Detector = {
    // type match { case ...}
    cfg.detectorType match {
      case "uniseas" => return new UniSeasDetector(cfg)
      case "ctx_ad_cross" => return new CrossCorrelDetector(cfg)
    }
  }
}

case class AnomalyStepDeletion(ctype:String, tid: String, pipeID: String, pipeVersion: String)
case class AnomalyStepConfigurationCommand(
                                            ctype:String,
                                            tid: String, pipeID: String, pipeVersion: String,
                                            outputTopic: String,
                                            detectors: Array[AnomalyStepConfiguration]
                                          )
case class AnomalyStepConfiguration(detectorID:String, detectorType: String, anomalyPercent: Double, dataFields:Array[String], crossCtxVar:String, windowSizeH: Double)

case class Anomaly(description:String)

@SerialVersionUID(123L)
class UniSeasDetector extends Detector with Serializable {

  @transient lazy val LOG = LoggerFactory.getLogger(classOf[UniSeasDetector])

  val COMPRESSION = 10
  var configParams: AnomalyStepConfiguration = null
  var mapToDigest: Map[(String, DayOfWeek, Int), AVLTreeDigest] = Map()
  var lastAnomalyTs= System.currentTimeMillis / 1000

  def this(cfg: AnomalyStepConfiguration)={
    this()
    this.configParams=cfg
  }

  override def isAnomaly(value: BumblebeeOutput): Option[Anomaly] = {
    if (configParams==null){
    LOG.error("Anomaly detector has nil config")
      return None
    }

    val ts: ZonedDateTime = ZonedDateTime.parse(value.ts)

    // println(s"Applying flatMap1 ${ts.toString}") // debug print
    // println(s"config params from Map1: ${configParams.value().getOrElse(None)}")

    /* extracting bounds from the percent configuration parameter*/
    val infPercent = configParams.anomalyPercent / (2 * 100)
    val supPercent = 1 - configParams.anomalyPercent / (2 * 100)
    // println(s"inf: ${infPercent}, sup: ${supPercent}") // debug print

    val f= configParams.dataFields(0)
    /* creates the key for the MapState for the current dataField */
    val quantilesKey: (String,  DayOfWeek, Int) = (value.assetID,  ts.getDayOfWeek, ts.getHour)
    /* Try to read the value for the given dataField or continue (and print error message to stderr) */
    val fieldValue: Double = (value.data \ f).validate[Double] match {
      case s: JsSuccess[Double] => {
        s.get
      }
      case e: JsError => {
        LOG.error("Error reading the data: it has to be a Double")
        return None
      }
    }
    // println(s"read data ${fieldValue}")  // debug print

    /* if the current key being processed do not exists, create the corresponding TDigest and add current data point and continue */
    if (!mapToDigest.contains(quantilesKey)) {
      LOG.debug(s"UniSeas: Creation of a new t-digest  for the key ${quantilesKey}")
      val tree: AVLTreeDigest = new AVLTreeDigest(COMPRESSION)
      tree.add(fieldValue)
      mapToDigest += (quantilesKey -> tree)
//      println("UniSeas: ",mapToDigest)
      return None
    }

    //debug
//    println(s"Uniseas: Median value for the key ${quantilesKey} is ${mapToDigest(quantilesKey).quantile(0.5)}")
    var res : Option[Anomaly] = None
    /* Updates the TDigest with new data and decide if it's an anomaly */
    if (fieldValue < mapToDigest(quantilesKey).quantile(infPercent)) {
      // add new datapoint to the TDigest
      val updatedAVLTreeDigest = mapToDigest(quantilesKey)
      updatedAVLTreeDigest.add(fieldValue)
      mapToDigest += (quantilesKey -> updatedAVLTreeDigest)
      val anomalyDesc=(s"UniSeas Alert: Unusually low count for asset ${quantilesKey._1}, field ${f} at time ${ts.toString}")
      LOG.info(anomalyDesc)
      res = Option(Anomaly(description = anomalyDesc))
    } else if (fieldValue > mapToDigest(quantilesKey).quantile(supPercent)) {
      // add new datapoint to the TDigest
      val updatedAVLTreeDigest = mapToDigest(quantilesKey)
      updatedAVLTreeDigest.add(fieldValue)
      mapToDigest += (quantilesKey -> updatedAVLTreeDigest)
      val anomalyDesc=s"UniSeas Alert: Unusually high count for asset ${quantilesKey._1}, field ${f} at time ${ts.toString}"
      LOG.info(anomalyDesc)
      res = Option(Anomaly(description = anomalyDesc))
    } else {
      val updatedAVLTreeDigest = mapToDigest(quantilesKey)
      updatedAVLTreeDigest.add(fieldValue)
      mapToDigest += (quantilesKey -> updatedAVLTreeDigest)
      res = None
    }
    LOG.debug(s"UniSeas: Tree size: ${mapToDigest(quantilesKey).size()}")
    //15 min debouncing and check t-digest size
    if (mapToDigest(quantilesKey).size() < 100){
      return None
    }else if(res.isDefined && (System.currentTimeMillis / 1000 - lastAnomalyTs > 60*15)) {
      lastAnomalyTs = System.currentTimeMillis / 1000
      //      println("UniSeas: Skipping alert phase")
      return res
    }else{
      return None
    }
  }

  override def updateCfg(config: AnomalyStepConfiguration): Unit = { //return boolean to detect failure ??
    if (config.dataFields(0)!=configParams.dataFields(0)){
      mapToDigest = Map()
    }
    configParams=config
  }

}

@SerialVersionUID(124L)
class CrossCorrelDetector extends Detector with Serializable {

  @transient lazy val LOG = LoggerFactory.getLogger(classOf[CrossCorrelDetector])

  val COMPRESSION = 10
  var tree: AVLTreeDigest = new AVLTreeDigest(COMPRESSION)
  var configParams:AnomalyStepConfiguration=null
  var lastAnomalyTs: Long = System.currentTimeMillis / 1000

  case class CorrData(ts: Long, x: Double, y: Double)

  var rollingWin: Array[CorrData] = Array()


  def this(cfg: AnomalyStepConfiguration)={
    this()
    this.configParams=cfg
  }

  override def isAnomaly(bbOut: BumblebeeOutput): Option[Anomaly] = {
    if (configParams==null){
      LOG.error("CrossCorrel: Anomaly detector has nil config")
      return None
    }
    val infPercent = configParams.anomalyPercent / (2 * 100)
    val supPercent = 1 - configParams.anomalyPercent / (2 * 100)
    // match asset output with weather data and store in an array (timestamp, val1, val2)
    var data: Array[Double] =  Array()
    (bbOut.data \ configParams.dataFields(0)).validate[Double] match {
      case s: JsSuccess[Double] => {
        data = data ++ Array(s.get)
      }
      case e: JsError => {
        LOG.error("CrossCorrel: Error reading the data field value: it has to be a Double")
        return None
      }
    }
    if (configParams.crossCtxVar==""){ //this is the self cross correl case
      if (configParams.dataFields.length != 2){
        LOG.error("CrossCorrel: invalid number of data fields")
        return None
      }
      (bbOut.data \ configParams.dataFields(1)).validate[Double] match {
        case s: JsSuccess[Double] => {
          data = data ++ Array(s.get)
        }
        case e: JsError => {
          LOG.error("CrossCorrel:Error reading the data field value: it has to be a Double")
          return None
        }
      }
    }else{
      if (configParams.dataFields.length != 1){
        LOG.error("CrossCorrel: invalid number of data fields")
        return None
      }
      //get the context variable
      bbOut.ctxData.transform(jspathFromDottedPath(configParams.crossCtxVar).json.pick) match{
        case JsSuccess(value, path)=>
         // println(path + "found")
          value.asOpt[Double] match{
            case Some(v)=>data = data ++ Array(v)
              case None=>{
                LOG.error(s"CrossCorrel: Context variable ${configParams.crossCtxVar} is not double")
                return None
              }
          }
        case e @ JsError(_) =>
          LOG.error(s"CrossCorrel: Looking for context variable ${configParams.crossCtxVar} Errors: ${JsError.toJson(e).toString()}")
          return None
      }
    }
    val newpoint = CorrData(bbOut.getLongTs(), data(0), data(1))
   // println("CrossCorrel: Asset ",bbOut.assetID,"Added new point to rolling win: ", newpoint.x, newpoint.y, rollingWin.length)
    rollingWin = rollingWin ++ Array(newpoint) // append new data to the rolling window
    val maxts = rollingWin.map(x => x.ts).max // find max ts
    val winLength = configParams.windowSizeH * 60 * 60 // in seconds
    rollingWin = rollingWin.filter(_.ts > maxts - winLength) // drop old data
   // println("CrossCorrel: Asset ",bbOut.assetID,"dropped rolling win data, new length: ", rollingWin.length)

    // compute xcorr
    var xx = 0.0
    var yy = 0.0
    var xy = 0.0
    for (e <- rollingWin){
      xy += e.x * e.y
      xx += e.x * e.x
      yy += e.y * e.y
    }
    val xcorr = xy/math.sqrt(xx * yy)
    // check for anomalies and update the t-digest
    if (xcorr.isNaN){ // this can be true when rollingWin is empty or when one between xx and yy is zero
      return None
    } else {
      tree.add(xcorr)
    }
    if (tree.size()<4000){ // minimum number of values in the t-digest to return an anomaly
//      println("CrossCorrel: Skipping alert phase")
      return None
    }
//    println("Rolling win len: ", rollingWin.length)
//    println("Correlation value: ", xcorr, "bounds for anomaly: ", tree.quantile(infPercent), tree.quantile(supPercent))

    // ***DEBUG
   /* val otherVariable = configParams.crossCtxVar match{
       case ""=>configParams.dataFields(1)
       case _ =>configParams.crossCtxVar
    }
    println(s"CrossCorrel: Median value of the correlation between ${configParams.dataFields(0)} and ${otherVariable} is ${tree.quantile(0.5)} for asset ${bbOut.assetID}")*/
    //***DEBUG

    if (xcorr < tree.quantile(infPercent) || xcorr > tree.quantile(supPercent)){
      //var anomalyDesc=("CrossCorrel: Unusual values between "+ configParams.dataFields(0)+" and ")
      val yVariable=configParams.crossCtxVar match{
        case ""=> configParams.dataFields(1)
        case _ => configParams.crossCtxVar
      }
      val anomalyDesc =
        s"""CrossCorrel: asset ${bbOut.assetID} is displaying an unusual behaviour:
           |${configParams.dataFields(0)} is ${data(0)} and ${yVariable} is ${data(1)}""".stripMargin.replaceAll("\n", " ")
      LOG.info(anomalyDesc)
      if(System.currentTimeMillis / 1000 - lastAnomalyTs < 60*15){return None} // debouncing: no other alerts for 15 minutes
      lastAnomalyTs= System.currentTimeMillis / 1000
      Option(Anomaly(description = anomalyDesc))
    }else {
      None
    }
  }

  override def updateCfg(config: AnomalyStepConfiguration): Unit = {
    //verify if fields changed, reset
    if (configParams.crossCtxVar!= config.crossCtxVar
      || configParams.dataFields.length!=config.dataFields.length
      || configParams.dataFields(0)!=config.dataFields(0)
      || (configParams.dataFields.length>1 && configParams.dataFields(1)!=config.dataFields(1))){
      //unsupported update, reset
      tree = new AVLTreeDigest(COMPRESSION)
    }

    configParams = config
  }


  def jspathFromDottedPath(dotted: String):JsPath={
    val components = dotted.split('.')
    //always assumes string is not empty
    dotted.split('.').headOption match {
      case Some(p) => components.tail.foldLeft(JsPath \ p) { _ \ _ }
      case _ => JsPath \ dotted
    }
  }
}

