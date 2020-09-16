package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/16 8:16 下午
 * 针对DataSet数据源的练习
 */

public class SourceTestDataSet {

    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据源
        // env.fromElements("")

        // ArrayList<String> list = new ArrayList<>();
        // list.add("陈子枢");
        // list.add("齐雷");
        // list.add("王海涛");
        // list.add("刘沛霞");
        // list.add("张慎政");
        // list.add("刘玉江");
        // list.add("董长春");
        // DataSource<String> source = env.fromCollection(list);
        DataSource<String> source = env.readTextFile("src/main/java/com/github/gudian1618/flinkvip/dataset/1.txt");
        // 3.转化
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        }).groupBy(0).sum(1)
        // 4.输出结果
        .print();
        // 5.
    }

}
