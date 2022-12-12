package com.nagi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hello").setMaster("master");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    }
}
