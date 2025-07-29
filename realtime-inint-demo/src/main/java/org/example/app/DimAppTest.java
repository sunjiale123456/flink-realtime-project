package org.example.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.example.bean.TableProcessDim;
import org.example.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DimAppTest {
    private static final Logger LOG = LoggerFactory.getLogger(DimAppTest.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

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
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/Idea/javaProject/chk/DimAppTest/");


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .scanNewlyAddedTableEnabled(true) // enable scan the newly added tables feature
                .databaseList("ods,dwd") // set captured database
                .tableList("ods.student,dwd.school,ods.student_info") // set captured tables [product, user, address]
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        SingleOutputStreamOperator<ArrayList<JSONObject>> processStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql")
                .map(value -> {
                            JSONObject jsonObj = JSONObject.parseObject(value);
                            String db = jsonObj.getJSONObject("source").getString("db");
                            String tb = jsonObj.getJSONObject("source").getString("table");
                            jsonObj.remove("source");
                            jsonObj.remove("transaction");
                            jsonObj.put("db", db);
                            jsonObj.put("tb", tb);
                            return jsonObj;
                        }
                )
                // 允许5秒的水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        // 从数据中提取时间戳
                        .withTimestampAssigner((jsonObject, timestamp) -> jsonObject.getLong("ts_ms"))
                        // 防止空闲分区阻塞水位线
                        .withIdleness(Duration.ofSeconds(3))
                )
                .keyBy(jsonObj -> jsonObj.getString("tb"))
                // 3秒钟的窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .trigger(new Trigger<JSONObject, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(JSONObject jsonObject, long l, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext ctx) throws Exception {

                    }
                })
                .process(new ProcessWindowFunction<JSONObject, ArrayList<JSONObject>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<JSONObject> iterable, Collector<ArrayList<JSONObject>> out) throws Exception {
                        Iterator<JSONObject> iterator = iterable.iterator();
                        ArrayList<JSONObject> list = new ArrayList<JSONObject>();
                        while (iterator.hasNext()){
                            list.add(iterator.next());
                        }
                        list.sort(Comparator.comparing(data -> data.getLong("ts_ms")));
                        out.collect(list);
                    }
                });


        OutputTag sinkMysqlTag = new OutputTag<JSONObject>("sinkMysqlTag") {};

        // 1. 配置文件源
        String filePath = "F:\\Idea\\javaProject\\flink-realtime-project\\realtime-inint-demo\\src\\main\\java\\config\\config.txt";
        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineFormat(), // 文本行格式
                new Path(filePath)) // 监控路径
                .monitorContinuously(java.time.Duration.ofMinutes(1)) // 监控间隔
                .build();

        // 2. 创建数据流
        SingleOutputStreamOperator<JSONObject> dbFieldStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(), // 水印策略
                "File Source"
        ).map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String data)  {
                        try {
                            return  JSONObject.parseObject(data);
                        }catch (Exception e){
                            LOG.error("DY-001： 维表数据格式有误 " + data + " 请更新加入 .... ");
                            return null;
                        }
                    }
                });
        MapStateDescriptor<String, JSONObject> mapStateDescriptor = new MapStateDescriptor<String,JSONObject>("mapStateDescriptor", TypeInformation.of(String.class),TypeInformation.of(JSONObject.class)){};
        BroadcastStream<JSONObject> broadcast = dbFieldStream.broadcast(mapStateDescriptor);
        SingleOutputStreamOperator<JSONObject> processSink = processStream.connect(broadcast).process(new BroadcastProcessFunction<ArrayList<JSONObject>, JSONObject, JSONObject>() {

            public final  MapStateDescriptor<String,JSONObject> metaStateDescriptor = new MapStateDescriptor<String,JSONObject>("metaStateDescriptor",TypeInformation.of(String.class),TypeInformation.of(JSONObject.class)){};

//            private transient MapState<String, JSONObject> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {


            }

            @Override
            public void processElement(ArrayList<JSONObject> jsonObjects, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
                for (JSONObject num : jsonObjects) {
                    String db = num.getString("db");
                    String tb = num.getString("tb");
                    String newKey = db + "." + tb;
                    ReadOnlyBroadcastState<String, JSONObject> mapState = ctx.getBroadcastState(mapStateDescriptor);
                    if (mapState.get(newKey)!=null) {
                        JSONObject metaObj = mapState.get(newKey);
                        JSONObject meta = metaObj.getJSONObject("meta");
                        JSONArray primary = metaObj.getJSONArray("primary");
                        num.put("meta", meta);
                        num.put("primary", primary);
                        out.collect(num);
                    } else {
                        ctx.output(sinkMysqlTag,num);
                        LOG.warn("DY-002： 写入目标表 " + newKey + " 不存在元数据信息，请更新加入 .... ");
                    }
                }

            }

            @Override
            public void processBroadcastElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
                String dataBase = jsonObject.getString("dbBase");
                String table = jsonObject.getString("table");
                String key = dataBase + "." + table;
                BroadcastState<String, JSONObject> mapState = ctx.getBroadcastState(mapStateDescriptor);
                mapState.put(key,jsonObject);
            }
        });

        processSink.addSink(new RichSinkFunction<JSONObject>() {

            private int batchSize = 10;
            private Connection connection = null;
            private PreparedStatement statement = null;
            private String jbcUrl = String.format("jdbc:mysql://localhost:3306/ods?&useSSL=false&rewriteBatchedStatements=true");

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(jbcUrl,"root","123456");
                connection.setAutoCommit(false);
            }

            @Override
            public void invoke(JSONObject dataObj, Context context) throws Exception {
                String op = dataObj.getString("op");
                switch (op) {
                    case "c":
                    case "r": insertFun(dataObj);
                    break;
                    case "u": updateFun(dataObj);
                    break;
                    case "d": deleteFun(dataObj);
                    break;
                }
            }

            @Override
            public void close() throws Exception {
                connection.close();
            }


            // 增加
            public void insertFun(JSONObject dataObj){
                String insertSql = formatInsertSql(dataObj);
                try {
                    statement=connection.prepareStatement(insertSql);
                    statement.executeUpdate();
                    connection.commit();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }

            }
            // {"op":"c","meta":{"score":"score","name":"varchar(255)","id":"int","age":"int"},
            // "after":{"score":23,"name":"aa","id":11,"age":67},"ts_ms":1752631371615,"db":"ods","tb":"student","primary":["id"]}
            // 更改
            public void updateFun(JSONObject dataObj) {
                String updateSql = formatUpdateSql(dataObj);
                try {
                    statement=connection.prepareStatement(updateSql);
                    statement.executeUpdate();
                    connection.commit();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

            // 删除
            public void deleteFun(JSONObject dataObj){
                String deleteSql = formatDeleteSql(dataObj);
                try {
                    statement=connection.prepareStatement(deleteSql);
                    statement.executeUpdate();
                    connection.commit();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }

            }
            public String formatInsertSql(JSONObject obj ){
                String db = obj.getString("db");
                String tb = obj.getString("tb");
                JSONObject after = obj.getJSONObject("after");
                Set<Map.Entry<String, Object>> entrySet = after.entrySet();
                StringBuffer fieldSql = new StringBuffer();
                StringBuffer valueSql = new StringBuffer();
                StringBuffer backups = new StringBuffer();

                int size = entrySet.size();
                int index=1;
                for(Map.Entry<String, Object> num :entrySet){
                    String field = num.getKey();
                    String value = num.getValue().toString();
                    if(index==size) {
                        fieldSql.append(field);
                        valueSql.append("'"+value+"'");
                        backups.append(field+" = VALUES("+field+")");
                    }else {
                        fieldSql.append(field+", ");
                        valueSql.append("'"+value+"', ");
                        backups.append(field+" = VALUES("+field+"), ");
                    }
                    index++;
                }
                String insert = "INSERT INTO "+db+"."+tb+"_sink ( "+fieldSql+") VALUE ("+valueSql+") " +
                                "ON DUPLICATE KEY UPDATE " +backups+";";

                LOG.info("数据写入语句 执行sql "+ insert);
                return insert;
            }

            public String formatUpdateSql(JSONObject obj){
                String db = obj.getString("db");
                String tb = obj.getString("tb");
                JSONArray primaryArr = obj.getJSONArray("primary");
                JSONObject meta = obj.getJSONObject("meta");
                String primary = primaryArr.size()>0?primaryArr.getString(0):"";
                String metaType = primary != null? meta.getString(primary).toUpperCase():null;
                JSONObject before = obj.getJSONObject("before");
                String condition = "";
                if (metaType.contains("INT")){
                    Integer primaryValue = before.getInteger(primary);
                    condition = primary+"="+primaryValue;
                }else if(metaType.contains("varchar")){
                    String primaryValue = before.getString(primary);
                    condition = primary+"='"+primaryValue+"'";
                }
                JSONObject after = obj.getJSONObject("after");
                Set<Map.Entry<String, Object>> entrySet = after.entrySet();
                StringBuffer valueSql = new StringBuffer();
                int size = entrySet.size();
                int index = 1;
                for(Map.Entry<String, Object> num :entrySet){
                    String field = num.getKey();
                    String value = num.getValue().toString();
                    if(index==size) {
                        valueSql.append(field+"='"+value+"'");
                    }else {
                        valueSql.append(field+"='"+value+"', ");
                    }
                    index++;
                }
                String update = "UPDATE "+db+"."+tb+"_sink "+" SET "+ valueSql +" WHERE "+ condition;
                LOG.info("数据更新语句 执行sql "+ update);

                return update;
            }

            public String formatDeleteSql(JSONObject obj) {
                String db = obj.getString("db");
                String tb = obj.getString("tb");
                JSONArray primaryArr = obj.getJSONArray("primary");
                JSONObject meta = obj.getJSONObject("meta");
                String primary = primaryArr.size()>0?primaryArr.getString(0):"";
                String metaType = primary != null? meta.getString(primary).toUpperCase():null;
                JSONObject before = obj.getJSONObject("before");
                String condition = "";
                if (metaType.contains("INT")){
                    Integer primaryValue = before.getInteger(primary);
                    condition = primary+"="+primaryValue;
                }else if(metaType.contains("varchar")){
                    String primaryValue = before.getString(primary);
                    condition = primary+"='"+primaryValue+"'";
                }
                String delete = "DELETE FROM "+db+"."+tb+"_sink "+" WHERE " +condition;
                LOG.info("数据删除语句 执行sql "+ delete);
                return delete;
            }

        });
        processSink.getSideOutput(sinkMysqlTag).print("未知数据：");
        env.execute();
    }
}
