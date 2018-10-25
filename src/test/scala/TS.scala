import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

/**
  * Junit 5 写法样例
  */
@TestInstance(Lifecycle.PER_CLASS)
class TS {

  lazy val tag = "o(╯□╰)o"

  @BeforeAll
  def init_all(): Unit = {
    println(s"$tag  init_all")
  }

  @BeforeEach
  def init(): Unit = {
    println(s"$tag  init")
  }

  @Test
  def ts_a(): Unit = {
    println(s"$tag  ts_a")
  }

  @Test
  def ts_b(): Unit = {
    println(s"$tag  ts_b")
  }

  @AfterEach
  def after(): Unit = {
    println(s"$tag  after")
  }

  @AfterAll
  def after_all(): Unit = {
    println(s"$tag  after_all")
  }

}
