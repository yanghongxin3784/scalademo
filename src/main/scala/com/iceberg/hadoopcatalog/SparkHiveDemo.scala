package com.iceberg.hadoopcatalog

import org.apache.spark.sql.SparkSession

/**
 * @Description:
 * @Author: YangHongXin
 * @Date: 2022/11/15 18:07
 * */
object SparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
    spark.sql("show databases").show()

  }
}
