package com.nagi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * RDD操作分为transformation和action，transformation从一个dataset创建新的dataset，action执行计算返回结果
 * 所有的transformation都是lazy的，在执行action之前，只是记忆应用到dataset的transformation
 * 默认情况，每次执行action都会基于最初的RDD重新计算，而不会基于上一次action的结果，但是可以通过persist和cache
 * 在第一次调用action时将执行完transformation后的RDD保存到集群内存或者硬盘，且cache具有容错性，当某个partition的数据丢失时，
 * Spark会使用最初的RDD和相关的transformation重新计算出该partition的数据
 * 涉及到RDD的操作会切分并分发到executors执行
 */
public class RDDDemo {
    public static void main(String[] args) {
        /**
         * initializing spark，指定app名称和master，master设置为local[n]，表示在本地进程运行spark进行调试
         * 也可指定master，例如，spark://192.168.56.101:7077
         * 生产环境一般使用submit提交，并通过--master参数指定master
         */
        SparkConf sparkConf = new SparkConf().setAppName("hello").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        /**
         * 复制集合元素转换为可以并行处理的distributed dataset
         * dataset会被分为多个partition，Spark为每个partition启动一个任务，partition的数量一般为
         * 集群内CPU的2-4倍，也可以通过第二个参数自定义partition数
         */
        JavaRDD<Integer> distData = sparkContext.parallelize(data);
        Integer sumResult = distData.reduce(Integer::sum);
        System.out.println("sumResult is " + sumResult);

        /**
         * 如果从文件系统读取文件，需要保证所有worker的相同路径下都有文件
         * Spark默认为每个block（默认128M）创建一个partition，也可以通过第二个参数自定义，但不能小于block数量
         */
        JavaRDD<String> distFile = sparkContext.textFile("data/data.txt");
        Integer fileLength = distFile.map(String::length).reduce(Integer::sum);
        System.out.println("File length is " + fileLength);

        /**
         * Spark在执行任务前，需要计算任务闭包（closure，包含对执行器可见的变量和方法），然后将序列化的闭包发送给执行器
         * 因此执行器操作的变量都是序列化在闭包中的复制数据，修改无法作用到当前JVM中的变量，只能访问变量的值
         * 如果需要在worker节点间共享变量，可以使用Accumulator
         */
        final int counter = 1;
        /**
         * 直接使用foreach访问元素，每个executor只能访问自己的元素，而不是driver的所有元素
         * 要想访问所有元素，需要调用collect()收集所有元素到driver，或者调用take()获取部分元素到driver，然后再访问
         */
        distData.collect().forEach(x -> System.out.println(x + counter));

        // 将元素转换为key,value对
        JavaPairRDD<String, Integer> pairs = distFile.mapToPair(line -> new Tuple2<>(line, 1));
        // 按照key聚合
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);
        // 打印结果
        counts.foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));

        // word count
        distFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                        .reduceByKey(Integer::sum)
                                .foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));

        /**
         * shuffle：all-to-all操作，Spark会读取所有partition的key的所有value，运算后再切分partition并分发
         * 触发shuffle的操作有repartition(e.g. repartition、coalesce),ByKey(e.g. groupByKey、reduceByKey),
         * join(e.g. cogroup、join)
         * persist可以指定存储级别，cache是MEMORY_ONLY()级别的persist
         * shuffle操作会自动调用persist保存中间数据
         */
        distFile.mapToPair(line -> new Tuple2<>(line, 1)).reduceByKey(Integer::sum).persist(StorageLevel.MEMORY_ONLY()).count();

        /**
         * 共享变量：broadcast variables，在每个机器缓存的只读变量，可以在同一机器上的tasks间共享，
         * 不然每个task只能读取自己closure内的变量
         */
        // 创建broadcast variable
        Broadcast<int[]> broadcast = sparkContext.broadcast(new int[]{1, 2, 3});
        // 读取broadcast variable
        int[] value = broadcast.value();
        // 清除broadcast variable，再次调用value时会再次广播
        broadcast.unpersist();
        // 永久删除broadcast variable
        broadcast.destroy();

        /**
         * 共享变量：accumulators，在集群中共享，任务不能读取值，只有driver可以执行value操作读取值
         * accumulator只支持numeric 类型，可以自定义其他类型
         * 当任务结束时，Spark会合并该任务对accumulator的更新到accumulator，但是会忽略可能发生的失败
         * 因此，即使accumulator更新合并失败，该任务任务的状态仍然会是successful
         * accumulator只在action更新，且每个任务只更新一次
         */
        LongAccumulator accumulator = sparkContext.sc().longAccumulator();
        sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(accumulator::add);
        System.out.println("acc value: " + accumulator.value());

        sparkContext.stop();
    }
}
