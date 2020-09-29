package com.github.gudian1618.flinkvip.datastream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        // 2.获取数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 3.转化
        // TODO
        // 4.输出数据
        source.print();
        // 5.触发执行
        env.execute("WordCountDataStream");
    }

}
