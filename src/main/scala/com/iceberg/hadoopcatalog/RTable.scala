package com.iceberg.hadoopcatalog

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.{HadoopCatalog, HadoopTables}
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.apache.iceberg.PartitionSpec

/**
 * @Description:
 * @Author: YangHongXin
 * @Date: 2022/11/9 20:44
 * */
object RTable {
  def main(args: Array[String]): Unit = {
   /* val warehousePath = "hdfs://flow-test1:8020/user/yhx/iceberg"
    val catalog = new HadoopCatalog(new Configuration, warehousePath)

    val ln  = catalog.listNamespaces()
    print(">>>>",ln.get(0))
    println(catalog.listTables(ln.get(0)))*/

    val tablePath = "hdfs://flow-test1:8020/user/yhx/sparkdemo"
    val tables = new HadoopTables(new Configuration())
    println(">>>>>>>>>>>>>>>>>",tables.exists(tablePath))
    val tab = tables.load(tablePath)
    println(tab.schema())

    //val trans  = tab.newTransaction()
    //trans.newAppend().appendFile()

  }

}
