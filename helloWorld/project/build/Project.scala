import sbt._

class Project(info: ProjectInfo) extends DefaultProject(info) {
  override val manifestClassPath = Some("scala-library.jar")
}

