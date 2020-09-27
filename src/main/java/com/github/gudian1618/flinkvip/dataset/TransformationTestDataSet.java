package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/19 10:41 下午
 * 针对Transformation练习
 */

public class TransformationTestDataSet {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据源
        // 可以通过控制并行度来进行计算
        DataSource<String> source =
            env.readTextFile("src/main/java/com/github/gudian1618/flinkvip/dataset/3.txt").setParallelism(1);
        // DataSource<String> source = env.fromElements("hadoop", "hive", "flume", "kafka", "flink");
        // 3.Transformation转化
        source.map(new MapFunction<String, Tuple5<String, String, String, String, Integer>>() {
                @Override
                public Tuple5<String, String, String, String, Integer> map(String value) throws Exception {
                    String[] s = value.split("\\|");
                    return new Tuple5<>(s[0],s[1],s[2],s[3],Integer.parseInt(s[4]));
                }
            }).project(2,0)
            // ==================================================
            // filter:过滤器,制作放行满足条件的数据(true)
            //     .filter(new FilterFunction<String>() {
            //     @Override
            //     public boolean filter(String value) throws Exception {
            //
            //         return value.split("\\|")[2].equals("中国");
            //     }
            // })
            // ========================================================================================
            // mapPartition:默认计算分区内的数据(并行度,之前的操作)
            //     .flatMap(new FlatMapFunction<String, String>() {
            //     @Override
            //     public void flatMap(String value, Collector<String> out) throws Exception {
            //         String[] split = value.split("\\|");
            //         for (String s : split) {
            //             out.collect(s);
            //         }
            //     }
            // }).mapPartition(new MapPartitionFunction<String, Long>() {
            //     @Override
            //     public void mapPartition(Iterable<String> values, Collector<Long> out) throws Exception {
            //         long l = 0;
            //         for (String value : values) {
            //             l++;
            //         }
            //         out.collect(l);
            //     }
            // })
            // ======================================================================================
            // flatMap:进一出多
            //     .flatMap(new FlatMapFunction<String, String>() {
            //     @Override
            //     public void flatMap(String value, Collector<String> collector) throws Exception {
            //         String[] split = value.split("\\|");
            //         for (int i = 0; i < split.length; i++) {
            //             collector.collect(split[i] + " " + i);
            //         }
            //
            //     }
            // })
            // ========================================================
            // map:进一出一
            //     .map(new MapFunction<String, Book>() {
            //     @Override
            //     public Book map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         Book book = new Book();
            //         book.setBookName(s[0]);
            //         book.setAuthor(s[1]);
            //         book.setCountry(s[2]);
            //         book.setGender(s[3]);
            //         book.setAge(Integer.parseInt(s[4]));
            //         return book;
            //     }
            // })
            // ==============================================================
            //     .map(new MapFunction<String, Tuple5<String, String, String, String, Integer>>() {
            //     @Override
            //     public Tuple5<String, String, String, String, Integer> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple5<>(s[0],s[1],s[2],s[3],Integer.parseInt(s[4]));
            //     }
            // })
            // 4.sink输出
            .print();
        // 5.提交文件

    }

}
