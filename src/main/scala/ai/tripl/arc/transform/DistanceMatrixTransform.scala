package ai.tripl.arc.transform

import ai.tripl.arc.api.API.{ARCContext, JupyterCompleter, PipelineStage, TransformPipelineStage}
import ai.tripl.arc.config.ConfigReader.{getOptionalValue, getValue}
import ai.tripl.arc.config.ConfigUtils.checkValidKeys
import ai.tripl.arc.config.Error.{Errors, StageError, stringOrDefault}
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.log.logger.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

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

object DistanceMatrixTransformStage {

  def execute(stage: DistanceMatrixTransformStage)(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext): Option[DataFrame] = {
    val df = spark.table(stage.inputView)
    Option(df)
  }
}
