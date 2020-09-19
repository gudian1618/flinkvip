package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/16 9:47 下午
 * 针对DataSet的sink数据输出的练习
 */

public class SinkTestDataSet {

    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据源
        DataSource<String> source = env.readTextFile("src/main/java/com/github/gudian1618/flinkvip/dataset/1.txt");
        // 3.转化
        AggregateOperator<Tuple2<String, Integer>> data = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String i : split) {
                    collector.collect(new Tuple2<>(i, 1));
                }
            }
        }).groupBy(0).sum(1);
        // 4.输出数据到本地文件
        // flink在运行时默认会调用服务器中的所有可见资源,默认使用最大化cpu核资源,如果需要单核一个进程需要调用设置setParallelism()
        data.writeAsText("src/main/java/com/github/gudian1618/flinkvip/dataset/2.txt").setParallelism(1);
        System.out.println("执行成功!");
        // 5.触发程序执行(当输出数据为文件时,必须要触发)
        env.execute("SinkTestDataSet");
    }

}
