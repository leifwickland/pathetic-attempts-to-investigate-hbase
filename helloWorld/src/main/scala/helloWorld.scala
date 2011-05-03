object helloWorld {
  def main(args: Array[String]) {
    println("Hello, world!  You sent me " + (if (args.length == 0) "(nothing)" else args.mkString(", ")))
  }
}
