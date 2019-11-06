package it.datasoil

import java.time.ZonedDateTime

import play.api.libs.json.JsObject


case class BumblebeeOutput(id:String,
                            pipeID: String,
                           pipeVersion: String,
                           adapterID: String,
                           tid: String,
                           assetID: String,
                           ts: String,
                           asset:AssetInfo,
                           data: JsObject,
                          ctxData: JsObject
                          ) {

  /** Parse the 'timestamp' parameter to a Long (to be used for timestamp extraction in the flink job)
    *
    * @return
    */
  def getLongTs(): Long = {
    try {
      ZonedDateTime.parse(this.ts).toEpochSecond
    } catch {
      case err: Throwable => {
        println("Unable to parse event time, defaulting to now")
        System.currentTimeMillis()/1000
      }
    }
  }

}


