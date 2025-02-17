object Queries {

  def killJackSparrow(t: Table): Option[Table] = Some(FilterRows(t, Not(Field("name", _ == "Jack"))).eval.getOrElse(t))

  def insertLinesThenSort(db: Database): Option[Table] = db.create("Inserted Fellas").tables.find(_.tableName == "Inserted Fellas").map(table => table.copy(tableData = List(Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"), Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"), Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"), Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172"))).sort("age"))
  
  def youngAdultHobbiesJ(db: Database): Option[Table] = db.join("People", "name", "Hobbies", "name").map(_.filter(Field("age", _.toInt < 25) && Field("name", _.startsWith("J")) && Field("hobby", _.nonEmpty)).select(List("name", "hobby")))

}
