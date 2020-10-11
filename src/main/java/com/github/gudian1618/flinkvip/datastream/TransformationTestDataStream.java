package com.github.gudian1618.flinkvip.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author gudian1618
 * @version v1.0
 * @date 2020/10/11 3:12 下午
 * 针对DataStream中Transformation转化的练习
 */

public class TransformationTestDataStream {

    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据源
        /*
        1|张三|18|男
        2|阿朱|12|女
        3|阿紫|11|女
        4|李四|22|男
        5|王五|19|男
        */
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 3.转化
        source
            // 封装为javabean
            .map(new MapFunction<String, User>() {
                @Override
                public User map(String value) throws Exception {
                    String[] s = value.split("\\|");
                    User user = new User();
                    user.setId(s[0]);
                    user.setName(s[1]);
                    user.setAge(Integer.parseInt(s[2]));
                    user.setGender(s[3]);
                    return user;
                }
            })
            .keyBy("gender")
            .timeWindow(Time.seconds(5))
            // .window(TumblingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
            .sum("age")
            // .max("age")
            // .sum("age")
            // .reduce(new ReduceFunction<User>() {
            // @Override
            // public User reduce(User value1, User value2) throws Exception {
            //     User user = value1;
            //     user.setAge(value1.getAge() + value2.getAge());
            //     return user;
            // }
            // })
            // 封装为tuple
            //     .map(new MapFunction<String, Tuple4<String, String, Integer, String>>() {
            //     @Override
            //     public Tuple4<String, String, Integer, String> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple4<>(s[0], s[1], Integer.parseInt(s[2]), s[3]);
            //     }
            // }).keyBy(3).reduce(new ReduceFunction<Tuple4<String, String, Integer, String>>() {
            //     @Override
            //     public Tuple4<String, String, Integer, String> reduce(Tuple4<String, String, Integer, String> value1, Tuple4<String, String, Integer, String> value2) throws Exception {
            //         return new Tuple4<>(value1.f0,value1.f1,value1.f2+value2.f2,value1.f3);
            //     }
            // }).project(2,3)
            // 4.输出数据
            .print();

        // 5.触发执行
        env.execute("TransformationTestDataStream");
    }
}
