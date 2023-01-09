package quantexa.interview

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewMainTest extends SparkTesting{

  "executeExercises" should "execute and show the outputs for each proposed exercise" in {
    QuantexaInterviewMain.executeExercises()
  }
}
