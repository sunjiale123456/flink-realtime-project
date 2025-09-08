package org.example.app;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.example.bean.Student;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TimerWriteMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.检查点相关的设置
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 3.设置job取消后是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4.两个检查点之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 5.设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,30000L));
        // 6.设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/Idea/javaProject/chk/TimerWriteMysql/");


        // 获取数据
        DataStreamSource<Tuple4<Integer,String,Integer,String>> stringDataStreamSource = env.addSource(new SourceFunction<Tuple4<Integer,String,Integer,String>>() {

            SimpleDateFormat format = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void run(SourceContext<Tuple4<Integer,String,Integer,String>> sourceContext) throws Exception {

            while (true){
//                for (int i = 0; i < 10; i++) {
//                    Tuple4<Integer, String, Integer, Integer> tp4;
//                    if (i< 3) {
//                       tp4 = Tuple4.of(i, "a", 100, i);
//                    }else if (i< 5){
//                       tp4 = Tuple4.of(i, "b", 200, i);
//                    }else {
//                       tp4 = Tuple4.of(i, "c", 300, i);
//                    }
//                    tp4 = Tuple4.of(i, "c", 300, i);
//                    sourceContext.collect(tp4);
//                }
                int i = (int) (Math.random() * 10);
                if (i>5){
                    sourceContext.collect(Tuple4.of(i,"a",1,format.format(System.currentTimeMillis())));
                }else {
                    sourceContext.collect(Tuple4.of(i,"b",1,format.format(System.currentTimeMillis())));
                }
                Thread.sleep(1000);
              }
            }

            @Override
            public void cancel() {

            }
        });

        // 使用处理函数定时写入
        stringDataStreamSource.keyBy(tp -> tp.f0)
                .process(new ProcessFunction<Tuple4<Integer, String, Integer, String>, String>() {
                    private Map<String,Boolean> openFlag =new HashMap<>();
                    private ValueState<Student> valueState;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Student> descriptor = new ValueStateDescriptor<>("student-real", Student.class);
                        valueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple4<Integer, String, Integer, String> tp4, Context context, Collector<String> out) throws Exception {
                        Student student = new Student(tp4.f0,tp4.f1, tp4.f2, tp4.f3);
                        valueState.update(student);
                        Boolean aBoolean = openFlag.get(tp4.f1);
                        if (aBoolean==null) {
                            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + Duration.ofSeconds(5).toMillis());
                            openFlag.put(tp4.f1,false);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        Student student = valueState.value();
                        String format = simpleDateFormat.format(new Date(timestamp));
                        out.collect(student.toString()+" "+format);
                        valueState.clear();
                        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+Duration.ofSeconds(5).toMillis());

                    }
                })
        .setParallelism(1)
        .print();

        env.execute();
    }
}
