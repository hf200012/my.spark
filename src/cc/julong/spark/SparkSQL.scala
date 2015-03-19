package cc.julong.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


case class Record(key: Int, value: String)


/**
 * Created by zhangfeng on 2015/2/10.
 */
object SparkSQL {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDDRelation")
    sparkConf.setMaster("spark://hadoop01:7077")
    val sc = new SparkContext(sparkConf)
    sc.textFile("/soft/people.txt")
    val sqlContext = new SQLContext(sc)

    import sqlContext._

    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    rdd.registerTempTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = sql("SELECT key, value FROM records WHERE key = 10")

    println("Result of RDD.map:")
    rddFromSql.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect.foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    rdd.where('key === 1).orderBy('value.asc).select('key).collect().foreach(println)

    // Write out an RDD as a parquet file.
    rdd.saveAsParquetFile("pair.parquet_2")

    // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
    val parquetFile = sqlContext.parquetFile("pair.parquet_2")

    // Queries can be run using the DSL on parequet files just like the original RDD.
    parquetFile.where('key === 1).select('value as 'a).collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerTempTable("parquetFile_1")
    sql("SELECT * FROM parquetFile_1").collect().foreach(println)


  }

}
