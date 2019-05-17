package com.luogh.learning.lab.flink
import scala.util.control.NonFatal

object ReflectionApp {

  def main(args: Array[String]): Unit = {
    val instance = ClassA("test")
    val result = reflectFieldValue(instance, "name")
    println(result)
  }

  def reflectFieldValue[T: Manifest](instance: T, fieldName: String): Any = {
    require(instance != null && fieldName != null && fieldName.nonEmpty)
    import scala.reflect.runtime.universe
    try {
      val reflectMirror =
        universe.runtimeMirror(getClass.getClassLoader).reflect(instance)
      val field = universe
        .typeOf[T]
        .decl(universe.TermName(fieldName))
        .asTerm
        .accessed
        .asTerm
      reflectMirror.reflectField(field).get
    } catch {
      case NonFatal(e) ⇒
        throw new RuntimeException(
          s"reflect field name ${fieldName} failed.",
          e
        )
    }
  }
}

case class ClassA(name: String) {
  def x(): Unit = println("x method")
}
