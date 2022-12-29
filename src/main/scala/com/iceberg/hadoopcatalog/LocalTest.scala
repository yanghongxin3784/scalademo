package com.iceberg.hadoopcatalog

import scala.io.Source
import java.sql.{Connection, DriverManager}
/**
 * @Description:
 * @Author: YangHongXin
 * @Date: 2022/12/7 9:59
 * */
object LocalTest {
  def main(args: Array[String]): Unit = {


    val list =  Source.fromFile("D:\\table1.txt").getLines().map{
      x=>
        val arr = x.split(" ")
        val col = arr(2).replaceAll("`","")
        val comment = arr(arr.length-1).replaceAll("'","").replaceAll(",","")
        (col,comment)
    }
    list.foreach(println)
  }

  def getConn(): Connection = {
     val driver = "com.mysql.cj.jdbc.Driver"
     val url = "jdbc:mysql://172.16.128.97:3306/shenlian?useUnicode=true&characterEncoding=utf-8&useSSL=false"
     val username = "flowai"
     val password = "flowai123"
      Class.forName(driver)
     DriverManager.getConnection(url, username, password)
  }

}
