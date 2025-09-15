package org.example.app;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StudentScoreAnalysis {
    private static final Logger LOG = LoggerFactory.getLogger(StudentActivityStatistics.class);


    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 假设输入数据格式: 学生ID,学生姓名,班级ID,得分,耗时(分钟)
        // 示例数据: "3,张3,01,5,2"
        DataStream<String> input = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true){
                    int id = (int)(Math.random()*10);
                    String  data = id+",王"+id+",01,"+(int)(Math.random()*5)+","+(int)(Math.random()*5);
                    sourceContext.collect(data);
                    LOG.info(data);
                    Thread.sleep(10000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        // 将输入数据转换为元组 学生ID,学生姓名,班级ID,得分,耗时(分钟)
        DataStream<Tuple5<String, String, String,Integer,Integer>> activityData = input.map(
                new MapFunction<String, Tuple5<String, String, String, Integer,Integer>>() {
                    @Override
                    public Tuple5<String, String, String, Integer,Integer> map(String value) throws Exception {
                        String[] parts = value.split(",");
                        String studentId = parts[0].trim();
                        String studentName = parts[1].trim();
                        String classId = parts[2].trim();
                        int score = Integer.parseInt(parts[3].trim());
                        int cost = Integer.parseInt(parts[4].trim());
                        return new Tuple5<String, String, String, Integer,Integer>(studentId, studentName, classId, score,cost);
                    }
                });

        // 按键分区（按学生ID）
        KeyedStream<Tuple5<String, String, String, Integer,Integer>, String> keyedData = activityData.keyBy(value -> value.f0);



        // 应用15分钟的滚动窗口（基于处理时间）
        DataStream<StudentActivityResult> result = keyedData.window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new WindowFunction<Tuple5<String, String, String, Integer,Integer>, StudentActivityResult, String, TimeWindow>() {
                    @Override
                    public void apply(String studentId, TimeWindow window,
                                      Iterable<Tuple5<String, String, String, Integer,Integer>> input,
                                      Collector<StudentActivityResult> out) throws Exception {

                        int totalScore = 0;
                        int totalTimeCost = 0;
                        int activityCount = 0;
                        String studentName = "";
                        String classId = "";

                        // 计算累计得分和耗时
                        for (Tuple5<String, String, String, Integer,Integer> activity : input) {
                            totalScore += activity.f3;
                            totalTimeCost+= activity.f4;
                            activityCount++;
                            studentName=activity.f1;
                            classId=activity.f2;
                        }

                        // 格式化窗口时间
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                        String windowStart = sdf.format(new Date(window.getStart()));
                        String windowEnd = sdf.format(new Date(window.getEnd()));

                        // 输出结果
                        out.collect(new StudentActivityResult(
                                studentId,
                                studentName,
                                classId,
                                windowStart,
                                windowEnd,
                                totalScore,
                                totalTimeCost,
                                activityCount
                        ));
                    }
                });

        // 打印结果到控制台
        result.addSink(new RichSinkFunction<StudentActivityResult>() {

            private transient Connection connection;

            @Override
            public void open(Configuration parameters) {
                try {
                    connection = DriverManager.getConnection(
                            "jdbc:mysql://localhost:3306/ods",
                            "root",
                            "123456"
                    );
                    connection.setAutoCommit(false);
                    LOG.info("数据库连接成功");
                }catch (Exception e){
                    LOG.info("数据库连接失败 "+ e.getMessage());
                }
            }
            @Override
            public void invoke(StudentActivityResult bean, Context context) {
                String studentId = bean.studentId;
                // 使用参数化查询防止SQL注入
                String selectSql = "SELECT id, score FROM ods.student_demo WHERE id = ?";

                try {
                    PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                    selectStmt.setString(1, studentId);

                    try (ResultSet rs = selectStmt.executeQuery()) {
                        if (rs.next()) {
                            // 存在记录则更新
                            String updateSql = "UPDATE ods.student_demo SET score = ? WHERE id = ?";
                            try (PreparedStatement updateStmt = connection.prepareStatement(updateSql)) {
                                int currentScore = rs.getInt("score");
                                updateStmt.setInt(1, currentScore + bean.totalScore);
                                updateStmt.setString(2, studentId);
                                updateStmt.executeUpdate();
                                LOG.info("数据更新成功: {}", bean);
                            }
                        } else {
                            // 不存在记录则插入
                            String insertSql = "INSERT INTO ods.student_demo(id, name, classId, score ,age) VALUES (?, ?, ?, ? ,?)";
                            try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
                                insertStmt.setString(1, bean.studentId);
                                insertStmt.setString(2, bean.studentName);
                                insertStmt.setString(3, bean.classId);
                                insertStmt.setInt(4, bean.totalScore);
                                insertStmt.setInt(5, 16);
                                insertStmt.executeUpdate();
                                LOG.info("数据插入成功: {}", bean);
                            }
                        }
                    }
                    connection.commit(); // 所有操作成功后才提交
                } catch (Exception e) {
                    try {
                        connection.rollback(); // 出错时回滚
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    LOG.error("数据库操作失败: {} - {}", bean, e.getMessage(), e);
                    throw new RuntimeException("数据库操作失败", e);
                }
            }

            @Override
            public void close() throws Exception {
                connection.close();
            }
        });

        // 执行作业
        env.execute("Student Activity Statistics (Processing Time)");
    }

    // Time)");
}