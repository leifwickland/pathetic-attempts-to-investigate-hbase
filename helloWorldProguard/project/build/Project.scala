import sbt._
import java.util.jar.Attributes.Name._


class Project(info: ProjectInfo) extends DefaultProject(info) with ProguardProject {
  // 
  // repositories
  //

  val scalaToolsSnapshots = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"

  //
  // dependencies
  //


  val ScalaCheck = "org.scala-tools.testing" %% "scalacheck" % "1.8" % "test"

  val ScalaTest = buildScalaVersion match {
    case "2.9.0.RC2" => "org.scalatest" % "scalatest" % "1.4.RC2" % "test"
    case "2.8.1"     => "org.scalatest" % "scalatest" % "1.3" % "test"
    case x           => error("Unsupported Scala version " + x)
  }

  val JUnit = "junit" % "junit" % "4.5" % "test"

  //
  // configuration
  //
  
  override def mainClass: Option[String] = Some("helloWorldProguard")

  override def compileOptions =
    Optimise :: Deprecation ::
    target(Target.Java1_5) ::
    Unchecked :: CompileOption("-no-specialization") ::
    super.compileOptions.toList

  override def packageOptions = ManifestAttributes(
    MAIN_CLASS -> "helloWorldProguard",
    IMPLEMENTATION_TITLE -> "HelloWorldProguard",
    IMPLEMENTATION_URL -> "http://helloWorldProguard.org/",
    IMPLEMENTATION_VENDOR -> "helloWorldProguard.org",
    SEALED -> "true") :: Nil

  override def managedStyle = ManagedStyle.Maven

  override def packageDocsJar = defaultJarPath("-javadoc.jar")

  override def packageSrcJar = defaultJarPath("-sources.jar")

  override def packageTestSrcJar = defaultJarPath("-test-sources.jar")

  lazy val sourceArtifact = Artifact(artifactID, "src", "jar", Some("sources"), Nil, None)

  lazy val docsArtifact = Artifact(artifactID, "docs", "jar", Some("javadoc"), Nil, None)

  override def compileOrder = CompileOrder.JavaThenScala

  override def packageToPublishActions =
    super.packageToPublishActions ++ Seq(packageDocs, packageSrc, packageTestSrc)
  
  override def allDependencyJars = (
    super.allDependencyJars +++ 
    Path.fromFile(buildScalaInstance.compilerJar) +++ 
    Path.fromFile(buildScalaInstance.libraryJar)
  )

  override def proguardOptions = List(
    proguardKeepMain("helloWorldProguard"),
    proguardKeepMain("helloWorldProguard$"),
    proguardKeepMain("scala.tools.nsc.MainGenericRunner"),
    "-dontoptimize",
    "-dontobfuscate", 
    proguardKeepLimitedSerializability,
    proguardKeepAllScala,
    "-keep class ch.epfl.** { *; }",
    "-keep interface scala.ScalaObject"
  )

}

