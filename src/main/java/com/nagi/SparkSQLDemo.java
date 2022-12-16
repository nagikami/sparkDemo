package com.nagi;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("SQLDemo").getOrCreate();
        /**
         * Dataset是结合了RDD（强类型、支持lambda）和Spark SQL（结构化数据处理、优化的执行引擎）优点的分布式数据集合
         * DataFrame是包含column的Dataset，类似于关系型数据库的表
         */
        Dataset<Row> df = spark.read().json("data/data.json");
        // Displays the content of the DataFrame to stdout
        df.show();
        // Print the schema in a tree format
        df.printSchema();
        // Select only the "name" column
        df.select("name").show();
        // Select everybody, but increment the age by 1
        df.select(new Column("name"), new Column("age").plus(1)).show();
        // Select people older than 21
        df.filter(new Column("age").gt(20)).show();
        // Count people by age
        df.groupBy("age").count().show();
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("user");
        Dataset<Row> sqlDf = spark.sql("select * from user");
        sqlDf.show();
        spark.stop();
    }
}
