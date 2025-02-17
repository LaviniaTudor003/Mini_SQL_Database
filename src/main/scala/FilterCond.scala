import scala.language.implicitConversions

trait FilterCond {def eval(r: Row): Option[Boolean]}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = r.get(colName).map(predicate)
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = conditions.map(_.eval(r))
    results match {
      case _ if results.contains(None) => None
      case _ => Some(results.map(_.get).reduce(op))
    }
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = f.eval(r).map(!_)
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = {
  new FilterCond {
    override def eval(r: Row): Option[Boolean] = {
      f1.eval(r) match {
        case Some(true) => f2.eval(r)
        case _ => Some(false)
      }
    }
  }
}
def Or(f1: FilterCond, f2: FilterCond): FilterCond = {
  new FilterCond {
    override def eval(r: Row): Option[Boolean] = {
      f1.eval(r) match {
        case Some(true) => Some(true)
        case _ => f2.eval(r)
      }
    }
  }
}
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = {
  new FilterCond {
    override def eval(r: Row): Option[Boolean] = {
      (f1.eval(r), f2.eval(r)) match {
        case (Some(v1), Some(v2)) => Some(v1 == v2)
        case _ => None
      }
    }
  }
}

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = fs.map(_.eval(r))
    results match {
      case _ if results.contains(None) => None
      case _ => Some(results.exists(_.get))
    }
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val results = fs.map(_.eval(r))
    results match {
      case _ if results.exists(_.isEmpty) => None
      case _ => Some(!results.exists(_.exists(_ == false)))
    }
  }
}

implicit def tuple2Field(t: (String, String => Boolean)): Field = {
  val (colName, predicate) = t
  Field(colName, predicate)
}

extension (f: FilterCond) {
  def ===(other: FilterCond) = Equal(f, other)
  def &&(other: FilterCond) = And(f, other)
  def ||(other: FilterCond) = Or(f, other)
  def !! = Not(f)
}