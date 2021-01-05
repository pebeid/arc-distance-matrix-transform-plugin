package ai.tripl.arc.transform

import ai.tripl.arc.api.API.{ARCContext, JupyterCompleter, PipelineStage, TransformPipelineStage}
import ai.tripl.arc.config.ConfigReader.{getOptionalValue, getValue}
import ai.tripl.arc.config.ConfigUtils.checkValidKeys
import ai.tripl.arc.config.Error.{Errors, StageError, stringOrDefault}
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.util.{DetailException, Utils}
import com.typesafe.config.Config
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.{HttpClients, LaxRedirectStrategy}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}

import scala.util.{Failure, Success, Try}

class DistanceMatrixTransform extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
                  |  "type": "DistanceMatrixTransform",
                  |  "name": "DistanceMatrixTransform",
                  |  "environments": [
                  |    "production",
                  |    "test"
                  |  ],
                  |  "APIKey": ${ENV_API_KEY},
                  |  "inputView": "inputView",
                  |  "originField": "fromAddress",
                  |  "destinationField": "toAddress",
                  |  "region": "au",
                  |  "outputView": "outputView"
                  |  "distanceField": "distanceM"
                  |}""".stripMargin
  val documentationURI = new java.net.URI(s"${baseURI}/transform/#httptransform")

  def instantiate(index: Int, config: Config)(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext)
  : Either[List[StageError], PipelineStage] = {
    implicit val c = config

    val expectedKeys = List[String]("type", "id", "name", "description", "environments", "APIKey", "inputView",
      "outputView", "originField", "destinationField", "region", "distanceField", "persist")
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val apiKey = getValue[String]("APIKey")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val originField = getValue[String]("originField")
    val destinationField = getValue[String]("destinationField")
    val region = getOptionalValue[String]("region")
    val distanceField = getValue[String]("distanceField")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, apiKey, inputView, outputView, originField, destinationField, region, distanceField, persist, invalidKeys) match {
      case (Right (id), Right (name), Right (description), Right (apiKey), Right (inputView), Right (outputView), Right (originField),
    Right (destinationField), Right (region), Right (distanceField), Right (persist), Right (invalidKeys) ) =>
        val stage = DistanceMatrixTransformStage(this, id, name, description, apiKey, inputView, outputView, originField,
          destinationField, region, distanceField, persist)
        stage.stageDetail.put("id", id)
        stage.stageDetail.put("name", name)
        stage.stageDetail.put("description", description)
        stage.stageDetail.put("apiKey", apiKey)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("originField", originField)
        stage.stageDetail.put("destinationField", destinationField)
        stage.stageDetail.put("region", region)
        stage.stageDetail.put("distanceField", distanceField)
        stage.stageDetail.put("persist", persist)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, apiKey, inputView, outputView, originField, destinationField, region, distanceField, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class DistanceMatrixTransformStage(
                                         plugin: DistanceMatrixTransform,
                                         id: Option[String],
                                         name: String,
                                         description: Option[String],
                                         apiKey: String,
                                         inputView: String,
                                         outputView: String,
                                         originField: String,
                                         destinationField: String,
                                         region: Option[String],
                                         distanceField: String,
                                         persist: Boolean,
                                       ) extends TransformPipelineStage {
  override def execute()(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext): Option[DataFrame] = {
    DistanceMatrixTransformStage.execute(this)
  }
}

case class OriginDestinationHashed(origin: String, destination: String, _region: String, _hashOfOD: Int)

object DistanceMatrixTransformStage {

  def isNotStringType(theType: DataType): Boolean = theType != StringType

  def execute(stage: DistanceMatrixTransformStage)(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext): Option[DataFrame] = {

    import spark.implicits._

    var df = spark.table(stage.inputView)
    val schema = df.schema
    val originalCols = df.columns
    // Get origin field metadata and validate type

    logger.debug().message(s"${stage.originField} field index: ${schema.fieldIndex(stage.originField)}")

    val originFieldIndex = try {
      schema.fieldIndex(stage.originField)
    } catch {
      case e: Exception => throw new Exception(s"""'${stage.originField}' is missing. inputView has: [${df.schema.map(_.name).mkString(", ")}].""") with DetailException {
        override val detail = stage.stageDetail
      }
    }
    logger.debug().message(s"${stage.originField} data type: ${schema(originFieldIndex).dataType}")

    schema(originFieldIndex).dataType match {
      case theType if isNotStringType(theType) => throw new Exception(s"""'${stage.originField}' is not of a type String.""") with DetailException {
        override val detail = stage.stageDetail
      }
      case StringType => "OK"
    }
    logger.debug().message(s"${stage.destinationField} field index: ${schema.fieldIndex(stage.destinationField)}")

    // Get destination field metadata and validate type
    val destinationFieldIndex = try {
      schema.fieldIndex(stage.destinationField)
    } catch {
      case e: Exception => throw new Exception(s"""'${stage.destinationField}' is missing. inputView has: [${df.schema.map(_.name).mkString(", ")}].""") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    logger.debug().message(s"${stage.destinationField} data type: ${schema(destinationFieldIndex).dataType}")

    schema(destinationFieldIndex).dataType match {
      case theType if isNotStringType(theType) => throw new Exception(s"""'${stage.destinationField}' is not of a type String.""") with DetailException {
        override val detail = stage.stageDetail
      }
      case StringType => "OK"
    }

    val regionVal = if(stage.region.isDefined) stage.region.get else ""

    val hashOriginDestinationUDF = udf((origin: String, destination: String, region: String) => s"""${origin}:${destination}:${regionVal}""".hashCode)

    df = df.withColumn("_region", lit(regionVal))
    df = df.withColumn("_hashOfOD", hashOriginDestinationUDF(df.col(stage.originField), df.col(stage.destinationField), lit(regionVal)))

    try {

      val originDestinationHash = df.select(
        col(stage.originField).as("origin"),
        col(stage.destinationField).as("destination"),
        col("_region"), col("_hashOfOD")).as[OriginDestinationHashed]

      val originDestinationDistance = originDestinationHash.mapPartitions(rowPartition => {
        val apiUrl = "https://maps.googleapis.com/maps/api/distancematrix/json"
        val httpClient: Try[HttpClient] = Try {
          val poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager()
          poolingHttpClientConnectionManager.setMaxTotal(50)
          HttpClients.custom.setConnectionManager(poolingHttpClientConnectionManager)
            .setRedirectStrategy(new LaxRedirectStrategy).build
        }

        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats = DefaultFormats

        rowPartition.map(row => {
          httpClient match {
            case Success(client) => {
              val uriBuilder: URIBuilder = new URIBuilder(apiUrl)
              uriBuilder.setParameter("origins", row.origin)
              uriBuilder.setParameter("destinations", row.destination)
              if(stage.region.isDefined) {
                uriBuilder.setParameter("region", row._region)
              }
              uriBuilder.setParameter("key", stage.apiKey)
              val httpGet = new HttpGet(uriBuilder.build())
              val response = client.execute(httpGet);
              response.getEntity match {
                case null => (row._hashOfOD, -1)
                case other => {
                  val jsonString = EntityUtils.toString(other)
                  val json = parse(jsonString).asInstanceOf[JObject]
                  val result = Try { (((json \ "rows")(0) \ "elements")(0) \ "distance" \ "value").extract[Int] }
                  val distance = result match {
                    case Success(dist) => dist
                    case Failure(_) => -1
                  }
                  (row._hashOfOD, distance)
                }
              }
            }

            case Failure(exception) => throw new Exception(s""" Could not set up HTTP client """)
          }

        })
      }).toDF("_distanceHash", "_distance")

      df = df.join(originDestinationDistance, df("_hashOfOD") === originDestinationDistance("_distanceHash"))
        .select((originalCols :+ "_distance").map(col): _*)
      df = df.withColumnRenamed("_distance", stage.distanceField)

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }


    if (arcContext.immutableViews) df.createTempView(stage.outputView) else df.createOrReplaceTempView(stage.outputView)

    if (!df.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(df.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.partitions.length))

      if (stage.persist) {
        spark.catalog.cacheTable(stage.outputView, arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(df.count))
      }
    }

    Option(df)

}

}
