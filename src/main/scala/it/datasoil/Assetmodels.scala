package it.datasoil

import org.bson.types.ObjectId
import org.mongodb.scala.bson.annotations.BsonProperty
import play.api.libs.json._

case class GeoInfo(@BsonProperty("_id")id:ObjectId,  `type`:Int)

object GeoInfo{
  lazy implicit val objectIdFormat: Format[ObjectId] = Format(
    Reads[ObjectId] {
      case s: JsString => if (ObjectId.isValid(s.value)) JsSuccess(new ObjectId(s.value)) else JsError()
      case _           => JsError()
    },
    Writes[ObjectId]((o: ObjectId) => JsString(o.toHexString))
  )
  lazy implicit val GeoInfoWrites = Json.writes[GeoInfo]
  lazy implicit val GeoInfoReads = Json.reads[GeoInfo]
}

case class GeoContainer(@BsonProperty("_id")id:ObjectId, `type`:Int, @BsonProperty("ancs")ancestors:List[GeoInfo]  )

object GeoContainer{
  lazy implicit val objectIdFormat: Format[ObjectId] = Format(
    Reads[ObjectId] {
      case s: JsString => if (ObjectId.isValid(s.value)) JsSuccess(new ObjectId(s.value)) else JsError()
      case _           => JsError()
    },
    Writes[ObjectId]((o: ObjectId) => JsString(o.toHexString))
  )
  lazy implicit val GeoContainerWrites = Json.writes[GeoContainer]
  lazy implicit val GeoContainerReads = Json.reads[GeoContainer]
}

case class AssetInfo(@BsonProperty("_id")id:ObjectId, cod:String, @BsonProperty("geoparent")geoparent:GeoContainer)

object AssetInfo{
  lazy implicit val objectIdFormat: Format[ObjectId] = Format(
    Reads[ObjectId] {
      case s: JsString => if (ObjectId.isValid(s.value)) JsSuccess(new ObjectId(s.value)) else JsError()
      case _           => JsError()
    },
    Writes[ObjectId]((o: ObjectId) => JsString(o.toHexString))
  )
  lazy implicit val AssetInfoWrites = Json.writes[AssetInfo]
  lazy implicit val AssetInfoReads = Json.reads[AssetInfo]
}

