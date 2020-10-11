package com.github.gudian1618.flinkvip.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/29 11:14 下午
 * 基于DataStream实现单词计数WordCount
 */

public class WordCountDataStream {

    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 3.转化
        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(0).sum(1)
        // 4.输出数据
        .print().setParallelism(1);
        // 5.触发执行
        env.execute("WordCountDataStream");
    }

}
