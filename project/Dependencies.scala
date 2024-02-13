import sbt._
object Dependencies {
  lazy val pureConfigVersion = "0.17.4"
  lazy val logbackVersion = "2.20.0"

  lazy val awsDynamoDbClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % "0.1.23"
  lazy val dynamoFormatters = "uk.gov.nationalarchives" %% "dynamo-formatters" % "0.0.1"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.2"
  lazy val mockito = "org.mockito" %% "mockito-scala" % "1.17.30"
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.48"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
}
