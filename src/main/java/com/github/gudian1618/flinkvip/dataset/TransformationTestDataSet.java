package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

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
        DataSource<String> source = env.readTextFile("src/main/java/com/github/gudian1618/flinkvip/dataset/3.txt");
        // 3.Transformation转化
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] split = value.split("\\|");
                for (int i = 0; i < split.length; i++) {
                    collector.collect(split[i] + " " + i);
                }

            }
        })
            //     .map(new MapFunction<String, Book>() {
            //
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
