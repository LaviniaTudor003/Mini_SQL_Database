type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  override def toString: String = {
    val csvHeader = header.mkString(",")
    val csvRows = data.map(row => header.map(col => row.getOrElse(col, "")).mkString(","))
    List(csvHeader) ++ csvRows mkString "\n"
  }
  def insert(row: Row): Table = {
    tableData match {
      case data if data.contains(row) => new Table(tableName, data)
      case _ => new Table(tableName, tableData :+ row)
    }
  }

  def delete(row: Row): Table = {
    new Table(tableName, tableData.filterNot(_ == row))
  }

  def sort(column: String): Table = {
    new Table(tableName, tableData.sortBy(row => row.get(column)))
  }

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedData = tableData.map {
    case row if f.eval(row).contains(true) => row ++ updates
    case row => row
    }
    new Table(tableName, updatedData)
  }

  def filter(f: FilterCond): Table = {
    val filteredData = tableData.filter(row => f.eval(row).contains(true))
    new Table(tableName, filteredData)
  }

  def select(columns: List[String]): Table = {
    val filteredData = tableData.map(row =>
      columns.foldLeft(Map[String, String]())((acc, col) => row.get(col) match {
        case Some(value) => acc + (col -> value)
        case None => acc
      })
    )
    new Table(tableName, filteredData)
  }

  def header: List[String] = tableData.headOption.map(_.keys.toList).getOrElse(List.empty)
  def data: Tabular = tableData
  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val lines = s.split("\n").toList
    val header = lines.headOption.getOrElse("").split(",").map(_.trim).toList
    val rows = lines.tail.map { line =>
      val values = line.split(",").map(_.trim)
      header.zip(values).toMap
    }
    Table(name, rows)
  }
}

extension (table: Table) {
  def apply(i: Int): Option[Row] = i match {
    case index if index >= 0 && index < table.tableData.length =>
      Some(table.tableData(index))
    case _ => None
  }
}
