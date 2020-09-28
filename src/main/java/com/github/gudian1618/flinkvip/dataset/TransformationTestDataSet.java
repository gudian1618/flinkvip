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

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.获取数据源
        // 可以通过控制并行度来进行计算
        // DataSource<String> source = env.readTextFile("src/main/java/com/github/gudian1618/flinkvip/dataset/3.txt").setParallelism(1);
        DataSource<String> source = env.fromElements("hadoop", "hive", "flume", "kafka", "flink");
        // ================  ===========================
        // ================  ===========================
        // ================  ===========================
        // ================  ===========================
        DataSource<String> source1 = env.fromElements("1|铁锤|22", "2|钢蛋|18", "3|琪琪|18");
        source1.first(2)
        // DataSource<String> source2 = env.fromElements("铁锤|北京", "钢蛋|上海", "狗蛋|上海");
        // MapOperator<String, Tuple3<String, String, String>> input1 = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
        //     @Override
        //     public Tuple3<String, String, String> map(String value) throws Exception {
        //         String[] s = value.split("\\|");
        //         return new Tuple3<>(s[0], s[1], s[2]);
        //     }
        // });
        // MapOperator<String, Tuple2<String, String>> input2 = source2.map(new MapFunction<String, Tuple2<String, String>>() {
        //     @Override
        //     public Tuple2<String, String> map(String value) throws Exception {
        //         String[] s = value.split("\\|");
        //         return new Tuple2<>(s[0], s[1]);
        //     }
        // });
        // ================ union:两个数据集中的数据的并集,要求两个数据字段必须一致 ===========================
        // DataSource<String> source1 = env.fromElements("1|铁锤|22", "2|钢蛋|18", "3|琪琪|18");
        // DataSource<String> source2 = env.fromElements("铁锤|北京", "钢蛋|上海", "狗蛋|上海");
        // // 字段必须相同
        // source1.union(source2)
        // ============== cross:笛卡尔积 =====================
        // DataSource<String> source1 = env.fromElements("1|铁锤|22", "2|钢蛋|18", "3|琪琪|18");
        // DataSource<String> source2 = env.fromElements("铁锤|北京", "钢蛋|上海", "狗蛋|上海");
        // source1.cross(source2)
        // ==================== coGroup:相当于详细版本的join,join直接输出结果,而coGroup可以控制细节 ================================
        // DataSource<String> source1 = env.fromElements("1|铁锤|22", "2|钢蛋|18", "3|琪琪|18");
        // DataSource<String> source2 = env.fromElements("铁锤|北京", "钢蛋|上海", "狗蛋|上海");
        // MapOperator<String, Tuple3<String, String, String>> input1 = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
        //     @Override
        //     public Tuple3<String, String, String> map(String value) throws Exception {
        //         String[] s = value.split("\\|");
        //         return new Tuple3<>(s[0], s[1], s[2]);
        //     }
        // });
        // MapOperator<String, Tuple2<String, String>> input2 = source2.map(new MapFunction<String, Tuple2<String, String>>() {
        //     @Override
        //     public Tuple2<String, String> map(String value) throws Exception {
        //         String[] s = value.split("\\|");
        //         return new Tuple2<>(s[0], s[1]);
        //     }
        // });
        // input1.coGroup(input2).where(1).equalTo(0).with(new CoGroupFunction<Tuple3<String, String, String>,
        //     Tuple2<String, String>, Tuple4<String, String, String, String>>() {
        //     @Override
        //     public void coGroup(Iterable<Tuple3<String, String, String>> first, Iterable<Tuple2<String, String>> second, Collector<Tuple4<String, String, String, String>> out) throws Exception {
        //         for (Tuple3<String, String, String> f : first) {
        //             for (Tuple2<String, String> s : second) {
        //                 out.collect(new Tuple4<>(f.f0, s.f1, f.f1, f.f2));
        //             }
        //         }
        //     }
        // })

            // =================== outerJoin:左右外连接,两个数据集合求并集,left左外,right右外,full两个的并集 ==============================
            // DataSource<String> source1 = env.fromElements("1|铁锤|22", "2|钢蛋|18","3|琪琪|18");
            // DataSource<String> source2 = env.fromElements("铁锤|北京", "钢蛋|上海", "狗蛋|上海");
            // MapOperator<String, Tuple3<String, String, String>> input1 = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            //     @Override
            //     public Tuple3<String, String, String> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple3<>(s[0], s[1], s[2]);
            //     }
            // });
            // MapOperator<String, Tuple2<String, String>> input2 = source2.map(new MapFunction<String, Tuple2<String, String>>() {
            //     @Override
            //     public Tuple2<String, String> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple2<>(s[0], s[1]);
            //     }
            // });
            // input1.leftOuterJoin(input2).where(1).equalTo(0).with(new JoinFunction<Tuple3<String, String, String>,
            //     Tuple2<String, String>, Tuple4<String,String,String,String>>() {
            //     @Override
            //     public Tuple4<String, String, String, String> join(Tuple3<String, String, String> first, Tuple2<String, String> second) throws Exception {
            //         return new Tuple4<>(first.f0, second==null?"未知":second.f1, first.f1, first.f2);
            //     }
            // })

            // ============= join:将两个DataSet数据集交叉部分的数据进行合并,合并为一个DataSet =============================
            // DataSource<String> source1 = env.fromElements("1|铁锤|22", "2|钢蛋|18");
            // DataSource<String> source2 = env.fromElements("铁锤|北京", "钢蛋|上海", "狗蛋|上海");
            // MapOperator<String, Tuple3<String, String, String>> input1 = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            //     @Override
            //     public Tuple3<String, String, String> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple3<>(s[0], s[1], s[2]);
            //     }
            // });
            // MapOperator<String, Tuple2<String, String>> input2 = source2.map(new MapFunction<String, Tuple2<String, String>>() {
            //     @Override
            //     public Tuple2<String, String> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple2<>(s[0], s[1]);
            //     }
            // });
            // input1.join(input2).where(1).equalTo(0).projectFirst(0).projectSecond(1).projectFirst(1, 2)

            // ================== distinct:去重,可以指定按照某个或某些字段进行对比,去除重复的字段 ==============================
            // DataSource<String> source = env.fromElements("1,2", "2,7", "3,5", "3,5");
            // source.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            //     @Override
            //     public Tuple2<Integer, Integer> map(String value) throws Exception {
            //         return new Tuple2<>(Integer.parseInt(value.split(",")[0]), Integer.parseInt(value.split(",")[1]));
            //     }
            // }).distinct(0,1)
            // ============== aggregate:从一组数据中挑选数据重新组织成一个数据进行输出,进多出一 ===========================
            // DataSource<String> source = env.fromElements("1,2", "2,7", "3,5");
            // source.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            //     @Override
            //     public Tuple2<Integer, Integer> map(String value) throws Exception {
            //         return new Tuple2<>(Integer.parseInt(value.split(",")[0]), Integer.parseInt(value.split(",")[1]));
            //     }
            // }).aggregate(Aggregations.SUM,0).and(Aggregations.MIN,1)
            // =========== reduceGroup:进多出多 ===============================
            // DataSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);
            // source.reduceGroup(new GroupReduceFunction<Integer, Integer>() {
            //     @Override
            //     public void reduce(Iterable<Integer> values, Collector<Integer> out) throws Exception {
            //         int sum = 0;
            //         for (Integer value : values) {
            //             sum += value;
            //             out.collect(sum);
            //         }
            //     }
            // })
            // 3.Transformation转化
            // ========= reduce:进多出一 ================================================
            // DataSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);
            // source.reduce(new ReduceFunction<Integer>() {
            //     @Override
            //     public Integer reduce(Integer value1, Integer value2) throws Exception {
            //         return value1 + value2;
            //     }
            // })
            // ====================================================================================
            //
            // .map(new MapFunction<String, Tuple5<String, String, String, String, Integer>>() {
            //     @Override
            //     public Tuple5<String, String, String, String, Integer> map(String value) throws Exception {
            //         String[] s = value.split("\\|");
            //         return new Tuple5<>(s[0],s[1],s[2],s[3],Integer.parseInt(s[4]));
            //     }
            //     // project可以嵌套,类似于select的嵌套查询,这里是嵌套获取上次获取的数据
            // }).project(2,0).project(1)
            // ==================================================
            // filter:过滤器,制作放行满足条件的数据(true)
            //     .filter(new FilterFunction<String>() {
            //     @Override
            //     public boolean filter(String value) throws Exception {
            //
            //         return value.split("\\|")[2].equals("中国");
            //     }
            // })
            // ========================================================================================
            // mapPartition:默认计算分区内的数据(并行度,之前的操作)
            //     .flatMap(new FlatMapFunction<String, String>() {
            //     @Override
            //     public void flatMap(String value, Collector<String> out) throws Exception {
            //         String[] split = value.split("\\|");
            //         for (String s : split) {
            //             out.collect(s);
            //         }
            //     }
            // }).mapPartition(new MapPartitionFunction<String, Long>() {
            //     @Override
            //     public void mapPartition(Iterable<String> values, Collector<Long> out) throws Exception {
            //         long l = 0;
            //         for (String value : values) {
            //             l++;
            //         }
            //         out.collect(l);
            //     }
            // })
            // ======================================================================================
            // flatMap:进一出多
            //     .flatMap(new FlatMapFunction<String, String>() {
            //     @Override
            //     public void flatMap(String value, Collector<String> collector) throws Exception {
            //         String[] split = value.split("\\|");
            //         for (int i = 0; i < split.length; i++) {
            //             collector.collect(split[i] + " " + i);
            //         }
            //
            //     }
            // })
            // ========================================================
            // map:进一出一
            //     .map(new MapFunction<String, Book>() {
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
            // ==============================================================
            // tuple
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
