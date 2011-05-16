import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val proguard = "org.scala-tools.sbt" % "sbt-proguard-plugin" % "0.0.5"
  val codefellow = "de.tuxed" % "codefellow-plugin" % "0.3"
}
