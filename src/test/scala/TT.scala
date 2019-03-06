import org.junit.jupiter.api.{DisplayName, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

import scala.util.Random

@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark模块")
class TT {


  @Test
  def tt(): Unit = {
   import com.wheels.common.types.Metric

    println(Metric.ACC.toString)



  }

}
