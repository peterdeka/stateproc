package it.datasoil

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{File, FileInputStream, ObjectInputStream}
import java.util

import it.datasoil.anomalydetector.{AnomalyStepConfiguration, Detector}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.state.api.OperatorTransformation
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import org.apache.flink.api.java._
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction


object Job {
  def main(args: Array[String]): Unit = {
    // set up the execution environment
    import org.apache.flink.configuration.GlobalConfiguration
    import org.apache.flink.core.fs.FileSystem
    FileSystem.initialize(GlobalConfiguration.loadConfiguration("/home/deka/flinkdev/stateproc/")) //needed for AWS S3 credentials in flink.conf
    val env = ExecutionEnvironment.getExecutionEnvironment
   //transform
  /*  val savepoint = Savepoint.load(env, "s3://syn-saas/flink-savepoints/savepoint-1abb65-934333e09183", new FsStateBackend("s3://syn-saas/flink-checkpoints"))
    val s = savepoint.readKeyedState("evts-adp-transform",new TransformersReaderFunction,Types.STRING,Types.STRING)*/
    //anomaly
    val savepoint = Savepoint.load(env, "s3://syn-saas/flink-savepoints/savepoint-6dfea0-ffd0d5021351", new FsStateBackend("s3://syn-saas/flink-checkpoints"))
    val s = savepoint.readKeyedState("alertmap",new AnomalyStateReaderFunction,Types.STRING,Types.STRING)
    s.print()

    // execute program
    env.execute("Flink Scala API Skeleton")
  }

def savepointFromSerialized(env:ExecutionEnvironment):Unit={
  val coll = new util.ArrayList[(String,String,mutable.Map[String, Map[String, Detector]],mutable.Map[String,AnomalyStepConfiguration],String)]()

  new File("/home/deka/flinkdev/stateproc/serialized/").listFiles.
    filter { f => f.isFile && f.getName.endsWith(".serialize") }.
    map(f=>{
      println(s"Adding ${f.getAbsolutePath}")
      val ois = new ObjectInputStream(new FileInputStream(f.getAbsolutePath))
      val stock = ois.readObject.asInstanceOf[(String,String,mutable.Map[String, Map[String, Detector]],mutable.Map[String,AnomalyStepConfiguration],String)]
      coll.add(stock)
      ois.close
    })


  val ds = env.fromCollection(coll)

  val transformation =
    OperatorTransformation
      .bootstrapWith[(String,String,mutable.Map[String, Map[String, Detector]],mutable.Map[String,AnomalyStepConfiguration],String)](ds)
      .keyBy(new KSelector).transform(new AlertMapBootStrapper())

  Savepoint
    .create(new FsStateBackend("s3://syn-saas/flink-checkpoints"), 128)
    .withOperator("alertmap", transformation)
    .write("s3://syn-saas/flink-savepoints/somewhere")
}

}


class KSelector extends KeySelector[(String,String,mutable.Map[String, Map[String, Detector]],mutable.Map[String,AnomalyStepConfiguration],String),String]{
  override def getKey(value: (String, String, mutable.Map[String, Map[String, Detector]], mutable.Map[String, AnomalyStepConfiguration], String)): String = {
    value._1
  }
}

class AlertMapBootStrapper extends KeyedStateBootstrapFunction[String, (String,String,mutable.Map[String, Map[String, Detector]],mutable.Map[String,AnomalyStepConfiguration],String)] {
   var mapToDetector: MapState[String, Map[String, Detector]] = null
  var detectorsConfigs: MapState[String,AnomalyStepConfiguration] = null

  var outputTopic : ValueState[String]= null

  // here we keep the current pipeline version for each pipeline connected to this adapterid in order to be queryable from outside

  var pipeStatus: MapState[String, String] = null

  override def open(parameters: Configuration): Unit = {
    mapToDetector= getRuntimeContext.getMapState(new MapStateDescriptor[String,Map[String, Detector]]("mapToDetector", classOf[String],classOf[Map[String,Detector]]))
    detectorsConfigs = getRuntimeContext
      .getMapState(new MapStateDescriptor[String, AnomalyStepConfiguration]("detectorsConfigs",classOf[String],classOf[AnomalyStepConfiguration]))
    outputTopic = getRuntimeContext.getState(new ValueStateDescriptor[String]("outputTopic", classOf[String]))
     val pipeStatusDescriptor = new MapStateDescriptor[String, String]("pipeline-status", classOf[String], classOf[String])
    pipeStatus = getRuntimeContext
      .getMapState(pipeStatusDescriptor)
  }

  @throws[Exception]
  override def processElement(value: (String, String, mutable.Map[String, Map[String, Detector]], mutable.Map[String, AnomalyStepConfiguration], String), context: KeyedStateBootstrapFunction[String, (String, String, mutable.Map[String, Map[String, Detector]], mutable.Map[String, AnomalyStepConfiguration], String)]#Context): Unit = {
    pipeStatus.put(value._1,value._2) //pipeID,pipeVersion
    outputTopic.update(value._5)

    for(v <- value._3){
      mapToDetector.put(v._1,v._2)
    }
    for(v <- value._4){
      detectorsConfigs.put(v._1,v._2)
    }
    println(s"Saving state for pipelineid ${value._1}")
  }
}

class AnomalyStateReaderFunction extends KeyedStateReaderFunction[String, String] {
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

  override def readKey(k: String, context: KeyedStateReaderFunction.Context, out: Collector[String]): Unit = {
    /* val i = state.iterator()
     while (i.hasNext) {
       val dmap = i.next()
       val k = dmap.getKey*/
    println("AAHAHA",k)
    out.collect(k)
    //}
  }

}


class TransformersReaderFunction extends KeyedStateReaderFunction[String, String] {
  var state: MapState[String,String] = null

  var transformersState: MapState[String, TransformStepConfiguration] = null
  /* override def open(parameters: Nothing): Unit = {
    state = getRuntimeContext.getMapState(pipeStatusDescriptor)
  }
*/
  override def open(parameters: Configuration): Unit = {
     val pipeStatusDescriptor = new MapStateDescriptor[String, String]("pipeline-status", classOf[String], classOf[String])
     val tstatedesc =  new MapStateDescriptor[String, TransformStepConfiguration]("transformers", classOf[String], classOf[TransformStepConfiguration])
    state = getRuntimeContext.getMapState(pipeStatusDescriptor)
    transformersState = getRuntimeContext.getMapState(tstatedesc)
  }

  override def readKey(k: String, context: KeyedStateReaderFunction.Context, out: Collector[String]): Unit = {
   /* val i = state.iterator()
    while (i.hasNext) {
      val dmap = i.next()
      val k = dmap.getKey*/
    println("AAHAHA",k)
      out.collect(k)
    //}
  }

}