package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/15 9:41 下午
 * 入门案例:单词统计
 * hadoop hive flume kafka hadoop zookeeper hadoop flink flink
 */

public class WordCountDataSet {

    public static void main(String[] args) throws Exception {
        // 1.获取execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.加载/创建初始数据
        DataSource<String> source = env.fromElements("hadoop hive flume kafka hadoop zookeeper hadoop flink flink");
        // 3.指定对比数据的转换
        // 将字符串用空格分割,分别每个元素输出
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String i : split) {
                    collector.collect(i);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).groupBy(0).sum(1)
        // 4.指定将计算结果放在何处
        .print();
        // 5.触发程序执行(离线Dataset不需要)


    }


}
