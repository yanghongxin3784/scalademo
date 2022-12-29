package com.iceberg.hadoopcatalog

import org.apache.spark.sql.SparkSession

/**
 * @Description:
 * @Author: YangHongXin
 * @Date: 2022/11/10 17:40
 * */
object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      //.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      /*.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "hdfs://flow-test1:8020/user/yhx/sparkdemo")
      */
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.hadoop_prod","org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type","hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse","hdfs://flow-test1:8020/user/yhx/warehouse")
      .getOrCreate()


    /*val sql = "CREATE TABLE hadoop_prod.db1.tab1 (id bigint, data string) USING iceberg"
    spark.sql(sql)
    val sql2 = "CREATE TABLE hadoop_prod.db2.tab2 (id bigint, name string) USING iceberg"
    spark.sql(sql2)
    val sql3 = "INSERT INTO hadoop_prod.db1.tab1 VALUES (7, 'a'), (8, 'b'), (9, 'c');"
    spark.sql(sql3)*/
    /*val sql4="SELECT * FROM hadoop_prod.db1.tab1"
    spark.sql(sql4).show()*/
    //spark.read.parquet("hdfs://flow-test1:8020/user/yhx/warehouse/db1/tab1/data/00077-2-9e2cbfcb-a29a-4f42-98b1-05d76bdc1ee3-00001.parquet").show()
    /*val sql5="CALL hadoop_prod.system.rewrite_data_files('db1.tab1')"
    spark.sql(sql5).show()*/
    /*val sql6="DELETE FROM hadoop_prod.db1.tab1 WHERE id in (1,2)"
    spark.sql(sql6).show()*/
   /* val sql7="UPDATE hadoop_prod.db1.tab1 SET data = 'a' WHERE id in (8,9)"
    spark.sql(sql7).show()*/

    /*val sql8="CALL hadoop_prod.system.expire_snapshots('db1.tab1', TIMESTAMP '2022-11-16 16:00:00.000', 100)"
    spark.sql(sql8).show()*/

    val sql9="SELECT * FROM hadoop_prod.db1.tab1.snapshots;"
    spark.sql(sql9).show()
  }


}
