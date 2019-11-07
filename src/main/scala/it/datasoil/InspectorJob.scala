package it.datasoil

import java.util.Date
import java.util.concurrent.TimeUnit

import org.mongodb.scala.{Document, MongoClient}
import it.datasoil.anomalydetector.{AnomalyStepConfiguration, CrossCorrelDetector, Detector, UniSeasDetector}
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration



object InspectorJob {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.configuration.GlobalConfiguration
    import org.apache.flink.core.fs.FileSystem

    val argsParams = ParameterTool.fromArgs(args)
    val isTest = argsParams.getBoolean("test", false)

    FileSystem.initialize(GlobalConfiguration.loadConfiguration("/home/ale/Lavoro/dev/github/stateproc")) //needed for AWS S3 credentials in flink.conf
    val env = ExecutionEnvironment.getExecutionEnvironment
    val savepoint = Savepoint.load(env, "s3://syn-saas/flink-savepoints/DIOKANE", new FsStateBackend("s3://syn-saas/flink-checkpoints"))
    val s = savepoint.readKeyedState("alertmap",new AnomalyStateReaderFunction)

    if (isTest) {

      s.map(new MapFunction[Document, String] {
        override def map(value: Document): String = value.toString()
      }).writeAsText("prova").setParallelism(1)

    } else {
      val mongoUri = argsParams.get("mongouri")
      val mongoDb = argsParams.get("db")
      val mongoCollection = argsParams.get("collection")

      s.flatMap(new FlatMapFunction[Document, String] {

        lazy val mongoSink = MongoClient(mongoUri).getDatabase(mongoDb).getCollection(mongoCollection)
        override def flatMap(value: Document, out: Collector[String]): Unit = {
          val f = mongoSink.insertOne(value).toFuture()
          Await.result(f, Duration(5L,  TimeUnit.SECONDS))
          out.collect("___")
        }
      }).print()


    }


    env.execute("Flink Scala API Skeleton")
  }


}



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
      out.collect(Utils.parseToDocument(k, currentKey, innerMap))
    }
  }
}

