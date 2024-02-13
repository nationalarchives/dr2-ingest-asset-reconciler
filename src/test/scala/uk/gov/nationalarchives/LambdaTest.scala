package uk.gov.nationalarchives

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor6}
import uk.gov.nationalarchives.Lambda.StateOutput
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils
import upickle.default
import upickle.default.{macroR, read}

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks {
  implicit val stateDataReader: default.Reader[StateOutput] = macroR[StateOutput]
  val dynamoServer = new WireMockServer(9005)

  override def beforeEach(): Unit = {
    dynamoServer.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    dynamoServer.stop()
  }

  private val defaultDocxChecksum = "f7523c5d03a2c850fa06b5bbfed4c216f6368826"
  private val defaultJsonChecksum = "a8cfe9e6b5c10a26046c849cd3776734626e74a2"
  private val defaultFileName = "TEST-ID"

  private val nonMatchingChecksumValue = "non-matchingChecksum"
  private val nonMatchingFileName = "non-matchingFileName"

  private val docxFileIdInList = List("a25d33f3-7726-4fb3-8e6f-f66358451c4e")
  private val jsonFileIdInList = List("feedd76d-e368-45c8-96e3-c37671476793")

  val contentObjectApiVsDdbStates: TableFor6[String, String, String, String, List[String], String] = Table(
    (
      "DDB docxChecksum",
      "DDB jsonChecksum",
      "DDB docx name",
      "DDB json name",
      "ids that failed to match",
      "reason for failure"
    ),
    (
      defaultDocxChecksum,
      defaultJsonChecksum,
      s"$defaultFileName.docx",
      nonMatchingFileName,
      jsonFileIdInList,
      "json file name doesn't match"
    ),
    (
      defaultDocxChecksum,
      defaultJsonChecksum,
      nonMatchingFileName,
      s"$defaultFileName.json",
      docxFileIdInList,
      "docx file name doesn't match"
    ),
    (
      defaultDocxChecksum,
      defaultJsonChecksum,
      nonMatchingFileName,
      nonMatchingFileName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx file name & json file name doesn't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      s"$defaultFileName.docx",
      s"$defaultFileName.json",
      jsonFileIdInList,
      "json checksum doesn't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      s"$defaultFileName.docx",
      nonMatchingFileName,
      jsonFileIdInList,
      "json checksum & json file name don't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      nonMatchingFileName,
      s"$defaultFileName.json",
      docxFileIdInList ++ jsonFileIdInList,
      "json checksum & docx file name don't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      nonMatchingFileName,
      nonMatchingFileName,
      docxFileIdInList ++ jsonFileIdInList,
      "json checksum, docx file name & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      s"$defaultFileName.docx",
      s"$defaultFileName.json",
      docxFileIdInList,
      "docx checksum doesn't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      s"$defaultFileName.docx",
      nonMatchingFileName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      nonMatchingFileName,
      s"$defaultFileName.json",
      docxFileIdInList,
      "docx checksum & docx file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      nonMatchingFileName,
      nonMatchingFileName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, docx file name & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      s"$defaultFileName.docx",
      s"$defaultFileName.json",
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum & json checksum don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      s"$defaultFileName.docx",
      nonMatchingFileName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, json checksum & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      nonMatchingFileName,
      s"$defaultFileName.json",
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, json checksum & docx file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      nonMatchingFileName,
      nonMatchingFileName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, json checksum, docx file name & json file name don't match"
    )
  )

  "handleRequest" should "return an error if the asset is not found in Dynamo" in {
    val testUtils = new ExternalServicesTestUtils(dynamoServer)
    testUtils.stubGetRequest(testUtils.emptyDynamoGetResponse)
    val testLambda = testUtils.TestLambda()
    val ex = intercept[Exception] {
      testLambda.handleRequest(testUtils.standardInput, testUtils.outputStream, null)
    }
    ex.getMessage should equal(s"No asset found for ${testUtils.assetId} from ${testUtils.batchId}")

    testLambda.verifyInvocationsAndArgumentsPassed(0, 0, 0, 0)
  }

  "handleRequest" should "return an error if the Dynamo entry does not have a type of 'Asset'" in {
    val testUtils = new ExternalServicesTestUtils(dynamoServer)
    testUtils.stubGetRequest(testUtils.dynamoGetResponse.replace(""""S": "Asset"""", """"S": "ArchiveFolder""""))
    testUtils.stubPostRequest(testUtils.emptyDynamoPostResponse)
    val testLambda = testUtils.TestLambda()
    val ex = intercept[Exception] {
      testLambda.handleRequest(testUtils.standardInput, testUtils.outputStream, null)
    }
    ex.getMessage should equal(s"Object ${testUtils.assetId} is of type ArchiveFolder and not 'Asset'")

    testLambda.verifyInvocationsAndArgumentsPassed(0, 0, 0, 0)
  }

  "handleRequest" should "return an error if there were no entities that had the asset name as the SourceId" in {
    val testUtils = new ExternalServicesTestUtils(dynamoServer)
    testUtils.stubGetRequest(testUtils.dynamoGetResponse)
    testUtils.stubPostRequest(testUtils.dynamoPostResponse)
    val testLambda = testUtils.TestLambda(entitiesWithIdentifier = IO.pure(Nil))
    val ex = intercept[Exception] {
      testLambda.handleRequest(testUtils.standardInput, testUtils.outputStream, null)
    }
    ex.getMessage should equal(s"No entity found using SourceId 'Test Name'")

    testLambda.verifyInvocationsAndArgumentsPassed(1, 0, 0, 0)
  }

  "handleRequest" should "return an error if no children are found for the asset" in {
    val testUtils = new ExternalServicesTestUtils(dynamoServer)
    testUtils.stubGetRequest(testUtils.dynamoGetResponse)
    testUtils.stubPostRequest(testUtils.emptyDynamoPostResponse)
    val testLambda = testUtils.TestLambda()
    val ex = intercept[Exception] {
      testLambda.handleRequest(testUtils.standardInput, testUtils.outputStream, null)
    }
    ex.getMessage should equal(s"No children were found for ${testUtils.assetId} from ${testUtils.batchId}")

    testLambda.verifyInvocationsAndArgumentsPassed(numOfGetBitstreamInfoRequests = 0)
  }

  "handleRequest" should "return a 'wasReconciled' value of 'false' and a 'No entity found' 'reason' if there were no Content Objects belonging to the asset" in {
    val testUtils = new ExternalServicesTestUtils(dynamoServer)
    val outputStream = testUtils.outputStream
    testUtils.stubGetRequest(testUtils.dynamoGetResponse)
    testUtils.stubPostRequest(testUtils.dynamoPostResponse)

    val testLambda = testUtils.TestLambda(contentObjectsFromReps = IO.pure(Nil))
    testLambda.handleRequest(testUtils.standardInput, outputStream, null)

    val stateOutput = read[StateOutput](outputStream.toByteArray.map(_.toChar).mkString)

    stateOutput.wasReconciled should equal(false)
    stateOutput.reason should equal(
      "There were no Content Objects returned for entity ref '354f47cf-3ca2-4a4e-8181-81b714334f00'"
    )

    testLambda.verifyInvocationsAndArgumentsPassed(numOfGetBitstreamInfoRequests = 0)
  }

  forAll(contentObjectApiVsDdbStates) {
    (docxChecksum, jsonChecksum, docxName, jsonName, idsThatFailed, reasonForFailure) =>
      "handleRequest" should s"return a 'wasReconciled' value of 'false' and a 'reason' message that contains " +
        s"these ids: $idsThatFailed if $reasonForFailure " in {
          val testUtils = new ExternalServicesTestUtils(dynamoServer)
          val outputStream = testUtils.outputStream
          val dynamoPostResponse = testUtils.dynamoPostResponse
            .replace(s""""S": "$defaultDocxChecksum"""", s""""S": "$docxChecksum"""")
            .replace(s""""S": "$defaultJsonChecksum"""", s""""S": "$jsonChecksum"""")
            .replace(s""""S": "$defaultFileName.docx"""", s""""S": "$docxName"""")
            .replace(s""""S": "$defaultFileName.json"""", s""""S": "$jsonName"""")

          testUtils.stubGetRequest(testUtils.dynamoGetResponse)
          testUtils.stubPostRequest(dynamoPostResponse)

          val testLambda = testUtils.TestLambda()

          testLambda.handleRequest(testUtils.standardInput, outputStream, null)

          val stateOutput = read[StateOutput](outputStream.toByteArray.map(_.toChar).mkString)

          stateOutput.wasReconciled should equal(false)
          stateOutput.reason should equal(
            s"Out of the 2 files expected to be ingested for assetId '68b1c80b-36b8-4f0f-94d6-92589002d87e', " +
              s"a checksum could not be found for: ${idsThatFailed.mkString(", ")}"
          )

          testLambda.verifyInvocationsAndArgumentsPassed()
        }
  }

  "handleRequest" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if COs could be reconciled" in {
    val testUtils = new ExternalServicesTestUtils(dynamoServer)
    val outputStream = testUtils.outputStream
    testUtils.stubGetRequest(testUtils.dynamoGetResponse)
    testUtils.stubPostRequest(testUtils.dynamoPostResponse)
    val testLambda = testUtils.TestLambda()

    testLambda.handleRequest(testUtils.standardInput, outputStream, null)

    val stateOutput = read[StateOutput](outputStream.toByteArray.map(_.toChar).mkString)

    stateOutput.wasReconciled should equal(true)
    stateOutput.reason should equal("")

    testLambda.verifyInvocationsAndArgumentsPassed()
  }
}
