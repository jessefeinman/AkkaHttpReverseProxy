object Test extends App{
  val test: String = new util.Random().alphanumeric.take(10000000).mkString
  System.gc()

  def timegc = {
    val s = System.nanoTime()
    System.gc()
    println(System.nanoTime() - s)
  }

  val s1 = System.nanoTime()
  val a = {
    val (lst, curr) = test.foldRight((List[String](), List[Char]())) {
      case (c, (lst, curr)) => c match {
        case c if c.isUpper && curr.nonEmpty =>
          ((c.toString :: curr).mkString :: lst, List.empty)
        case _ => (lst, c :: curr)
      }
    }
    if (curr.nonEmpty) curr.mkString :: lst
    else lst
  }
  print(s"fr  ${System.nanoTime()-s1}\t")
  timegc

  val s2 = System.nanoTime()
  val b = {
    val (lst, curr) = test.foldLeft((List[String](), "")) {
      case ((lst, curr), c) => c match {
        case c if c.isUpper && curr.nonEmpty =>
          (curr.reverse.mkString :: lst, c.toString)
        case _ => (lst, c.toString + curr)
      }
    }
    (curr.reverse.mkString :: lst).reverse
  }
  print(s"fls ${System.nanoTime()-s2}\t")
  timegc

  val s3 = System.nanoTime()
  val c = {
    val (lst, curr) = test.foldLeft((List[String](), List[Char]())) {
      case ((lst, curr), c) => c match {
        case c if c.isUpper && curr.nonEmpty =>
          (curr.reverse.mkString :: lst, List(c))
        case _ => (lst, c :: curr)
      }
    }
    (curr.reverse.mkString :: lst).reverse
  }
  print(s"fll ${System.nanoTime()-s3}\t")
  timegc

  val s4 = System.nanoTime()
  val d = test.split("(?=[A-Z])")
  print(s"rx  ${System.nanoTime()-s4}\t")
  timegc

}
