case class Database(tables: List[Table]) {
  override def toString: String = {
    tables.map(_.toString).mkString("\n")
  }

  private def tableExists(tableName: String): Boolean = {
    tables.exists(_.tableName == tableName)
  }

  def create(tableName: String): Database = {
    tableExists(tableName) match {
      case true => this
      case false => Database(tables ++ List(Table(tableName, List.empty)))
    }
  }

  def drop(tableName: String): Database = {
    tableExists(tableName) match {
      case true => Database(tables.filterNot(_.tableName == tableName))
      case false => this
    }
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    def allTablesExist(tableNames: List[String]): Boolean = {
      tableNames.forall(tableExists)
    }

    tableNames match {
      case _ if allTablesExist(tableNames) =>
        val selectedTables = tables.filter(table => tableNames.contains(table.tableName))
        Some(Database(selectedTables))
      case _ => None
    }
  }


  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    val optTable1 = tables.find(_.tableName == table1)
    val optTable2 = tables.find(_.tableName == table2)

    val t1 = optTable1.get
    val t2 = optTable2.get

    val adjustedTable2Data = t2.tableData.map { row =>
      row.get(c2) match {
        case Some(value) => row - c2 + (c1 -> value)
        case None => row
      }
    }

    val commonValues = t1.tableData.flatMap(row1 =>
      adjustedTable2Data.collect {
        case row2 if row1(c1) == row2(c1) => (row1, row2)
      }
    )

    val combinedRows = commonValues.map {
    case (row1, row2) =>
      val filteredRow2 = row2.view.filterKeys(_ != c1).toMap

      val combinedRow = (row1.keySet ++ filteredRow2.keySet).map { key =>
        val value1 = row1.getOrElse(key, "")
        val value2 = filteredRow2.getOrElse(key, "")

        key -> (if (value1 == value2) {
          if (value1.isEmpty) "NULL" else value1
        } else {
          List(value1, value2).filterNot(_.isEmpty).mkString(";")
        })
      }.toMap
      combinedRow
    }

    val rowsOnlyInTable1 = t1.tableData.filterNot(row1 =>
      commonValues.exists { case (r1, _) => r1 == row1 }
    )

    val rowsOnlyInTable2 = adjustedTable2Data.filterNot(row2 =>
      commonValues.exists { case (_, r2) => r2 == row2 }
    )

    val allRows = combinedRows ++ rowsOnlyInTable1 ++ rowsOnlyInTable2
    val resultTableName = s"Joined_${t1.tableName}_${t2.tableName}"

    Some(Table(resultTableName, allRows))
}

  // Implement indexing here
  def apply(i: Int): Table = {
    tables.zipWithIndex.find { case (_, index) => index == i } match {
      case Some((table, _)) => table
      case None => Table("", List.empty)
    }
  }
}
