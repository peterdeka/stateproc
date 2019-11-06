package it.datasoil

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory

/*object Main extends App {
  implicit val TransformationReads = Json.reads[Transformation]
  implicit val TransformationDescriptor = Json.reads[TransformationDescriptor]

}*/

case class TransformStepDeletion(ctype:String, adapterID: String, tid: String, pipeID: String, pipeVersion: String)
case class TransformStepConfiguration(
                                       ctype:String,
                                       adapterID: String, tid: String, pipeID: String, pipeVersion: String,
                                       tsField: String, assetIdField: String, outputTopic: String, generateID: Boolean,
                                       transforms:Array[Transformation]
                                     ) {
  //private var  jsTransforms :Array[Array[Reads[JsObject]]]=new Array[Array[Reads[JsObject]]](0)
  lazy val LOG = LoggerFactory.getLogger(classOf[TransformStepConfiguration])

  def transform(original:JsObject, rawMode:Boolean):Option[Array[JsObject]]={
    var mainObject=Array[JsObject](Json.obj())
    transforms.foreach(t=>{
      var lastIterRes = Array[JsObject]()
      mainObject.foreach(jo=>{
        applyTransformationOperator(original,jo,t) match{
          case Some(x)=>lastIterRes=lastIterRes++x
          case _=>
        }
      })
      if (lastIterRes.length<1){
        return None
      }
      mainObject=lastIterRes
    })

    //inject expected fields and map fixed fields envelope, original message goes into 'data' branch
    val fixedFieldsTransform=(
      jspathFromDottedPath(tsField).json.prune andThen
        jspathFromDottedPath(assetIdField).json.prune andThen
        (__ \ 'adapterID).json.prune andThen
        ((__ \ 'data).json.copyFrom((__).json.pick)) and
        ((__ \ 'ts).json.copyFrom(jspathFromDottedPath(tsField).json.pick)) and
        ((__ \ 'assetID).json.copyFrom(jspathFromDottedPath(assetIdField).json.pick)) and
        (__ \ 'adapterID).json.put(JsString(adapterID)) and
        (__ \ 'tid).json.put(JsString(tid))and
        (__ \ 'pipeID).json.put(JsString(pipeID))and
        (__ \ 'pipeVersion).json.put(JsString(pipeVersion))
    )reduce

    //inject expected fields and map fixed fields, everything at root level
    val rawFixedFieldsTransform=(
      /*jspathFromDottedPath(tsField).json.prune andThen
        jspathFromDottedPath(assetIdField).json.prune andThen
        (__ \ 'adapterID).json.prune andThen*/
        ((__ ).json.copyFrom((__).json.pick)) and
       // ((__ \ 'ts).json.copyFrom(jspathFromDottedPath(tsField).json.pick)) and
      //  ((__ \ 'assetID).json.copyFrom(jspathFromDottedPath(assetIdField).json.pick)) and
        (__ \ 'adapterID).json.put(JsString(adapterID)) and
        (__ \ 'tid).json.put(JsString(tid))and
        (__ \ 'pipeID).json.put(JsString(pipeID))and
        (__ \ 'pipeVersion).json.put(JsString(pipeVersion))
      )reduce

    mainObject=mainObject.flatMap(x=>{
      val transformResult = rawMode match {
        case false => x.transform(fixedFieldsTransform)
        case true => x.transform(rawFixedFieldsTransform)
      }

      transformResult match{
         case JsSuccess(value, p)=>Some(value) //++x
         case e @ JsError(_)=>
           LOG.error("Errors: {}",JsError.toJson(e).toString())
           return None
       }
    })
    return Option(mainObject)
  }

  def applyTransformationOperator(original :JsObject,curObj :JsObject, operator:Transformation):Option[Array[JsObject]]={
    var tres = curObj
    val path= jspathFromDottedPath(operator.path)
    var transformationSource = original
    if (operator.path.startsWith("$$")) transformationSource=curObj
    operator.opType match{
      case "map"=>
        val jsonTransformer = jspathFromDottedPath(operator.newKey).json.copyFrom( path.json.pick )
        transformationSource.transform(jsonTransformer) match{
          case JsSuccess (value, path)=>
           // println(path + "mapped")
            tres= tres ++ value
          case e @ JsError(_) =>
            LOG.error("Errors: {}", JsError.toJson(e).toString())
            return None
        }
      case "filter"=>
        transformationSource.transform(path.json.pick) match{
          case JsSuccess(value, path)=>
           // println(path,"found")
            val v =value.asOpt[String] //to string as we actually only support filtering with string
            v match{
              case Some(vv)=>{
                if (!operator.params.contains(vv)){
                  LOG.debug(s"${path.toJsonString} value ${vv} not passing filter")
                  return None
                }
                tres=curObj //useless
                LOG.debug("filter passing")
              }
              case None=>{
                LOG.error("{} filtering is not supported as non string",path)
                return None
              }
            }

          case e @ JsError(_) =>
            LOG.error("Errors: {}", JsError.toJson(e).toString())
            return None
        }
      case "unwind"=>
        transformationSource.transform(path.json.pick) match {
          case JsSuccess(value, path) =>
            //println(path + "unwinding found")
            var resArray = Array[JsObject]()
            val nPath = jspathFromDottedPath(operator.newKey)
            value.asOpt[JsArray] match{
              case Some(x)=>x.value.foreach(x=>{
                tres.transform(nPath.json.put(x)) match{
                  case JsSuccess(value, path)=>
                    resArray :+=  tres ++ value
                  case e @ JsError(_) =>
                    LOG.error("Errors: {}", JsError.toJson(e).toString())
                    return None
                }
              })
                //println("returning array unwindeds")
                return Option(resArray)
              case _=>
                LOG.error("{} unwinding not array",path)
                return None
            }
          case e @ JsError(_) =>
            LOG.error("Errors: {}", JsError.toJson(e).toString())
            return None
        }
      case _=> return None
    }
    return Option(Array[JsObject](tres))
  }

  def jspathFromDottedPath(dotted: String):JsPath={
    //remove the transformation prefix if the case
    var cleanStr = dotted
    if (dotted.startsWith("$$") && dotted.length>2){
      cleanStr=dotted.substring(2)
    }
    val components = cleanStr.split('.')
    //always assumes string is not empty
    components.headOption match {
      case Some(p) => components.tail.foldLeft(JsPath \ p) { _ \ _ }
      case _ => JsPath \ cleanStr
    }
  }

}

// Transformation describes a single transformation step
case class Transformation (newKey:String,opType:String, path: String,params:Array[String])

/*
// ConductorFixedFieldsConfiguration contains the names of the fixed fields we must map to
case class ConductorFixedFieldsConfiguration (tsFieldName: String, assetIdFieldName: String, tenantIdFieldName: String)
*/
