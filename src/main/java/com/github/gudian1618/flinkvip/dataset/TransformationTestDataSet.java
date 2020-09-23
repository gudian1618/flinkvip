package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import scala.Tuple5;

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
        source.map(new MapFunction<String, Tuple5<String, String, String, String, Integer>>() {
            @Override
            public Tuple5<String, String, String, String, Integer> map(String value) throws Exception {
                String[] s = value.split("\\|");
                return new Tuple5<>(s[0],s[1],s[2],s[3],Integer.parseInt(s[4]));
            }
        })
            // 4.sink输出
            .print();
        // 5.提交文件

    }

}
