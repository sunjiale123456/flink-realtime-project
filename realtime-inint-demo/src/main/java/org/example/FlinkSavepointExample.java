package org.example;

import com.alibaba.fastjson.JSONObject;;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import org.apache.flink.util.Collector;


import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

public class FlinkSavepointExample {
    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境配置
        Configuration configuration = new Configuration();

        // 设置保存点路径
        String path = "file:///F:/data/save";
        String savepointPath = getSavePath("F:/data/save");
        if (StringUtils.isNotBlank(savepointPath)) {
            configuration.setString("execution.savepoint.path", "file:///"+savepointPath);
        }

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        // 配置检查点
        env.enableCheckpointing(5000); // 每10秒触发一次检查点
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置状态后端（这里使用文件系统作为示例）
        env.setStateBackend(new FsStateBackend(path));

        // 你的数据流处理逻辑...

//        DebeziumSourceFunction<String> sourceFunction = com.ververica.cdc.connectors.mysql.MySqlSource.<String>builder()
//                .hostname("localhost")
//                .port(3306)
//                .username("root")
//                .password("123456")
//                .databaseList("ods")
//                .tableList("ods.student")
//                .deserializer(new StringDebeziumDeserializationSchema())
//                .serverTimeZone("Asia/Shanghai")
//                .startupOptions(StartupOptions.initial())
//                .build();
//        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
        DataStream<String> dataStreamSource = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

                while (true) {
                    UUID uuid = UUID.randomUUID();
                    String key = uuid.toString().substring(1, 2);
                    long l = System.currentTimeMillis();
                    sourceContext.collect("" +
                            "{\"time\":\""+l+"\",\"key\":\""+key+"\"}" +
                            "");
                    Thread.sleep(100l);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<String> countStateStream = dataStreamSource.keyBy(data-> JSONObject.parseObject(data).getString("key")).map(new RichMapFunction<String, String>() {

            // ‌敏感信息‌：当对象包含敏感信息（如密码、密钥等）时，可以使用transient关键字来防止这些信息被序列化到字节流中，从而避免敏感信息的泄露
            private transient ValueState<Integer> countState ;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("countState", Integer.class);

                countState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public String map(String s) throws Exception {
                Integer count = countState.value();
                if (count == null) {
                    count = 0;
                }
                count++;
                countState.update(count);
                JSONObject object = JSONObject.parseObject(s);
                String dateTime = this.format.format(new Date(object.getLong("time")));
                object.put("count",count);
                object.putIfAbsent("dateTime",dateTime);
                return object.toJSONString();
            }
        });

        // 统计每5秒钟产生，平均次数
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> aggregateStream = countStateStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return JSONObject.parseObject(s).getLong("time");
                    }
                })
                .keyBy(data -> JSONObject.parseObject(data).getString("key"))
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .aggregate(new AggregateFunction<String, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> createAccumulator() {
                        return Tuple3.of(null, 0, 0);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> add(String s, Tuple3<String, Integer, Integer> input) {
                        JSONObject object = JSONObject.parseObject(s);
                        input.f0 = object.getString("key");
                        input.f1 += 1;
                        input.f2 += object.getInteger("count");
                        return input;
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> getResult(Tuple3<String, Integer, Integer> input) {
                        return Tuple3.of(input.f0, input.f1, input.f2 / input.f1);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> acc2, Tuple3<String, Integer, Integer> acc1) {
                        if (acc1.f0 == null) {
                            return acc2;
                        } else {
                            return acc1;
                        }
                    }
                });

        aggregateStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .windowAll(GlobalWindows.create()).trigger(new Trigger<Tuple3<String, Integer, Integer>, GlobalWindow>() {

            private int maxCount = 50;

            private long timeInternal = 10000;

            ValueStateDescriptor<Integer>  descriptorCount = new ValueStateDescriptor<>("count", Types.INT);

            ValueStateDescriptor<Long>  descriptorTime = new ValueStateDescriptor<>("timer", Types.LONG);

            @Override
            public TriggerResult onElement(Tuple3<String, Integer, Integer> input, long l, GlobalWindow globalWindow, TriggerContext ctx) throws Exception {

                ValueState<Integer> partitionedState = ctx.getPartitionedState(descriptorCount);

                int count = 0;
                if (partitionedState.value() ==null){
                    partitionedState.update(1);
                }else {
                    count =  partitionedState.value() +1 ;
                    partitionedState.update(count);
                }
                if(count>=maxCount){
                    return getResult(ctx);
                }


                ValueState<Long> currentTime = ctx.getPartitionedState(descriptorTime);
                if(currentTime.value() == null){
                    long nextTimer = ctx.getCurrentProcessingTime()+timeInternal;
                    ctx.registerProcessingTimeTimer(nextTimer);
                    currentTime.update(nextTimer);
                }
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, GlobalWindow globalWindow, TriggerContext ctx) throws Exception {
                long nextTimer = time + timeInternal;
                ctx.registerProcessingTimeTimer(nextTimer);
                ctx.getPartitionedState(descriptorTime).update(nextTimer);
                return getResult(ctx);
            }

            @Override
            public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

                triggerContext.getPartitionedState(descriptorCount).clear();
                triggerContext.getPartitionedState(descriptorTime).clear();

            }
            public TriggerResult getResult(TriggerContext ctx){
                ctx.getPartitionedState(descriptorCount).clear();
                return TriggerResult.FIRE_AND_PURGE;
            }

        })
                .process(new ProcessAllWindowFunction<Tuple3<String, Integer, Integer>, Integer, GlobalWindow>() {

                    ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count",Types.INT);

                    private  ValueState<Integer> count ;

                    @Override
                    public void process(Context context, Iterable<Tuple3<String, Integer, Integer>> ite, Collector<Integer> collector) throws Exception {
                        count = getRuntimeContext().getState(descriptor);
                        Iterator<Tuple3<String, Integer, Integer>> tup3 = ite.iterator();
                        while (tup3.hasNext()){
                            if(count.value() == null) {
                                count.update(0);
                            }
                            count.update(count.value()+ tup3.next().f2);
                        }
                        collector.collect(count.value());

                    }
                }).print();

        // 启动Flink任务
        env.execute("Flink Savepoint Example");
    }
    public static String getSavePath(String path){
        File file = new File(path);
        File[] fileList = null ;
        long max = 0;
        if(file.isDirectory()) {
            for (File data : file.listFiles()) {
                long time = data.lastModified();
                if (time > max) {
                    max = time;
                    fileList = data.listFiles();
                }
            }
        }

        String savePath = null ;
        if (fileList != null) {
            for (File data : fileList) {
                try {
                    if (data.getCanonicalPath().contains("chk")) {
                        savePath = data.getCanonicalPath();
                        break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return savePath;
    }
}

