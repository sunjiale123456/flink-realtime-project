package org.example.app;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class StudentScoreAnalysis {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 简化输出，生产环境可增加并行度

        // 2. 创建模拟数据源 - 实际应用中替换为真实数据源（如Kafka/Socket）
        DataStream<StudentScore> inputStream = env.addSource(new SourceFunction<StudentScore>() {
            @Override
            public void run(SourceContext<StudentScore> sourceContext) throws Exception {
                sourceContext.collect(new StudentScore());

            }

            @Override
            public void cancel() {

            }
        })
                .name("student-score-source");

        // 3. 处理数据流
        inputStream
                // 使用处理时间语义（根据业务需求可改为事件时间）
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StudentScore>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
                )
                // 按班级分组 - 如果所有学生属于同一个班级，可以用.keyBy(x -> "class_all")
                .keyBy(StudentScore::getClassId)
                // 15分钟滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(15)))
                // 聚合计算总分和总耗时
                .aggregate(new ScoreAggregator())
                // 输出到控制台
                .addSink(new PrintSinkFunction<>())
                .name("console-sink");

        // 4. 执行作业
        env.execute("学生得分滚动统计");
    }

    // 学生得分数据POJO类
    public static class StudentScore {
        private String studentName;
        private String studentId;
        private String classId;   // 班级ID
        private int score;
        private int timeSeconds;  // 耗时（秒）

        public StudentScore() {}  // Flink需要无参构造函数

        public StudentScore(String studentName, String studentId, String classId, int score, int timeSeconds) {
            this.studentName = studentName;
            this.studentId = studentId;
            this.classId = classId;
            this.score = score;
            this.timeSeconds = timeSeconds;
        }

        // Getters and Setters
        public String getStudentName() { return studentName; }
        public String getStudentId() { return studentId; }
        public String getClassId() { return classId; }
        public int getScore() { return score; }
        public int getTimeSeconds() { return timeSeconds; }

        @Override
        public String toString() {
            return String.format("%s(%s) - 得分: %d, 耗时: %ds",
                    studentName, studentId, score, timeSeconds);
        }
    }

    // 自定义聚合函数：计算总分和总耗时
    public static class ScoreAggregator implements AggregateFunction<
            StudentScore,
            Tuple2<Integer, Integer>,  // 累加器类型：(总分, 总耗时)
            Tuple2<Integer, Integer>   // 输出类型：(总分, 总耗时)
            > {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);  // 初始化(总分=0, 总耗时=0)
        }

        @Override
        public Tuple2<Integer, Integer> add(StudentScore value, Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(
                    accumulator.f0 + value.getScore(),
                    accumulator.f1 + value.getTimeSeconds()
            );
        }

        @Override
        public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> accumulator) {
            return accumulator;  // 直接返回累加结果
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);  // 合并并行计算结果
        }
    }

    // 模拟数据生成器 - 实际应用中替换为真实数据源
    public static class StudentScoreGenerator implements SourceFunction<StudentScore> {
        private volatile boolean isRunning = true;

        // 模拟学生数据
        private static final String[] STUDENT_NAMES = {"张三", "李四", "王五", "赵六", "钱七"};
        private static final String[] CLASS_IDS = {"classA", "classB"}; // 班级ID

        @Override
        public void run(SourceContext<StudentScore> ctx) throws Exception {
            while (isRunning) {
                // 随机生成学生得分记录

                String name = STUDENT_NAMES[(int) (Math.random()*(STUDENT_NAMES.length))];
                String studentId = "S" + (1000 + Math.random()*(9000));
                String classId = CLASS_IDS[(int) (Math.random()*(CLASS_IDS.length))];
                int score = 1 + (int)(Math.random()*10);       // 得分1-10
                int time = 1 + (int)(Math.random()*120);       // 耗时1-120秒

                ctx.collect(new StudentScore(name, studentId, classId, score, time));

                // 每0.5-2秒生成一条记录
                Thread.sleep(500 + (int)(Math.random()*1500));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}