package it.datasoil

import java.util.concurrent.TimeUnit

import org.mongodb.scala.{Document, MongoClient}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.state.api.Savepoint
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
    val savepoint = Savepoint.load(env, "s3://syn-saas/flink-savepoints/savepoint-6fe1d4-454129444a1c", new FsStateBackend("s3://syn-saas/flink-checkpoints"))
    val s = savepoint.readKeyedState("alertmap",new AnomalyStateReaderFunction)

    if (isTest) {

      s.map(new MapFunction[Document, String] {
        override def map(value: Document): String = value.toString()
      }).writeAsText("prova").setParallelism(1)

    } else {
      val mongoUri = argsParams.get("mongouri")
      val mongoDb = argsParams.get("db")
      val mongoCollection = argsParams.get("collection")

      s.output(new MongoSink[Document](mongoUri,mongoDb,mongoCollection))
    }


    env.execute("Flink Scala API Skeleton")
  }


}

class MongoSink[T]( databaseURI: String, databaseName:String, collectionName: String) extends OutputFormat[Document] {
  @transient var dbCli: MongoClient=null
  @transient lazy val LOG = LoggerFactory.getLogger(classOf[AnomalyStateReaderFunction])

  override def configure(parameters: Configuration): Unit = {

  }


  override def writeRecord(record: Document): Unit = {
    if(dbCli==null) LOG.warn("*******ONNNNNNNNN****NUUUUUUUUUUUUUUL")

   val d = dbCli.getDatabase(databaseName).getCollection(collectionName).insertOne(record)
     /*d.subscribe(new Observer[Completed] {
     override def onNext(result: Completed): Unit = LOG.warn(s"onNext: $result")
     override def onError(e: Throwable): Unit = LOG.warn(s"onError: $e")
     override def onComplete(): Unit = LOG.warn("onComplete")
   })*/
    Await.result(d.head(), Duration(5L,  TimeUnit.SECONDS))
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    if(dbCli==null) dbCli = MongoClient(databaseURI)
  }

  override def close(): Unit = {
 //   Thread.sleep(8000L)
    if (dbCli != null) dbCli.close
  }

}

