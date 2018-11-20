import org.junit.jupiter.api.{DisplayName, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

@TestInstance(Lifecycle.PER_CLASS)
@DisplayName("测试Spark模块")
class TT {


  @Test
  def tt(): Unit = {
    println(Seq("abc").mkString(","))
    println(Seq("def","cc").mkString(","))

  }

}
