package com.iceberg.hadoopcatalog

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Types
import org.apache.iceberg.{PartitionSpec, Schema, Table};

/**
 * @Description:
 * @Author: YangHongXin
 * @Date: 2022/11/9 20:42
 * */
object CTable {
  def main(args: Array[String]): Unit = {

    val tables = new HadoopTables(new Configuration())

    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.StringType.get),
      Types.NestedField.required(2, "name", Types.StringType.get),
      Types.NestedField.required(3, "event_time", Types.TimestampType.withZone),
      Types.NestedField.optional(4, "text", Types.StringType.get)
    )

    val spec = PartitionSpec.builderFor(schema)
      .hour("event_time")
      .identity("id").build

    //每个table路径得一致，理论上不需要太多的table
    val tablePath = "hdfs://flow-test1:8020/user/yhx/flowpplog"
    tables.create(schema,spec,tablePath)

  }

}
