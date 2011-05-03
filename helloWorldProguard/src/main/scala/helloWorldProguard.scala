object helloWorldProguard {
  def main(args: Array[String]) {
    println("Hello, World!  You sent me " + (if (args.length == 0) "(nothing)" else args.mkString(", ")))
  }
}
