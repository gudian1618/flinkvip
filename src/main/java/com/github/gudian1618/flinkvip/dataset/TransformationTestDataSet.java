package com.github.gudian1618.flinkvip.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/9/19 10:41 下午
 * 针对Transformation练习
 */

public class TransformationTestDataSet {

    public static void main(String[] args) {

        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据源
        DataSource<String> source = env.readTextFile("3.txt");
        // 3.Transformation转化


    }

}
