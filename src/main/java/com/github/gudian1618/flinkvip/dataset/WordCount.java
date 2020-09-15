package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/15 9:41 下午
 * 入门案例:单词统计
 * hadoop hive flume kafka zookeeper hadoop flink flink
 */

public class WordCount {

    public static void main(String[] args) {
        // 1.获取execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.加载/创建初始数据
        DataSource<String> source = env.fromElements("hadoop hive flume kafka zookeeper hadoop flink flink");
        //


    }


}
