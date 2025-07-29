package org.example.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.TableProcessDim;
import org.example.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.JdbcUtil;

import java.sql.Connection;
import java.util.*;


public class DimApplication {
    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10002);
        // TODO 2.检查点相关设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);

        // 1.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 3.设置job取消后是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4.两个检查点之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 5.设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,30000L));
        // 6.设置状态后端
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(Constant.HDFS_DIR+"dimApplication");
        // 7.设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","sunjiale");

        // TODO 3.获取kafka业务数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics("topic")
                .setGroupId("dimApplication")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<JSONObject> kafkaSourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka Source")
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return jsonObject;
                    }
                });

        // TODO 4.获取mysql变更数据
        // 获取数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .scanNewlyAddedTableEnabled(true) // enable scan the newly added tables feature
                .databaseList("db") // set captured database
                .tableList("db.product, db.user, db.address") // set captured tables [product, user, address]
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();
        SingleOutputStreamOperator<TableProcessDim> mysqlSourceStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Flink cdc from Mysql")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String record) throws Exception {
                        JSONObject jsonOb = JSON.parseObject(record);
                        String op = jsonOb.getString("op");
                        TableProcessDim tableProcessDim = "d".equals(op)?jsonOb.getObject("before",TableProcessDim.class):jsonOb.getObject("after",TableProcessDim.class);
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                });
        // 创建状态描述器
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String,TableProcessDim>("mapStateDescriptor",TypeInformation.of(String.class),TypeInformation.of(TableProcessDim.class)){};
        // 将数据进行广播
        BroadcastStream<TableProcessDim> broadcast = mysqlSourceStream.broadcast(mapStateDescriptor);

        // TODO 4.处理逻辑
        // 主流数据连接广播数据
        kafkaSourceStream.connect(broadcast)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {

                    private HashMap<String ,TableProcessDim> configMap  = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection connection = JdbcUtil.getJdbcMysqlConnection();
                        List<TableProcessDim> tableProcessDimList = JdbcUtil.QuerySqlList(connection, Constant.MYSQL_QUERY_STR, TableProcessDim.class, true);
                        for(TableProcessDim tableProcessDim : tableProcessDimList){
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }
                        JdbcUtil.closeJdbcMysqlConnection(connection);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        // 获取业务数据 判断是否是 配置数据，关联写入下游
                        String table = jsonObject.getString("table");
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastStateMap = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        TableProcessDim tableProcessDim = configMap.get(table) != null ? configMap.get(table):broadcastStateMap.get(table);
                        if (tableProcessDim !=null ){
                            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据

                            // 将维度数据继续向下游传递(只需要传递data属性内容即可)
                            JSONObject jsonObjectData = jsonObject.getJSONObject("data");

                            //在向下游传递数据前，过滤掉不需要传递的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            List<String> columnList = Arrays.asList(sinkColumns.split(","));
                            Set<String> keySet = jsonObjectData.keySet();
                            keySet.removeIf(data -> ! columnList.contains(data) );



                            //在向下游传递数据前，补充对维度数据的操作类型属性
                            String type = jsonObject.getString("type");
                            jsonObjectData.put("type",type);

                            out.collect(Tuple2.of(jsonObjectData,tableProcessDim));

                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim, Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        // 获取新的变更数据
                        String sourceTable = tableProcessDim.getSourceTable();
                        String op = tableProcessDim.getOp();
                        BroadcastState<String, TableProcessDim> broadcastStateMap = context.getBroadcastState(mapStateDescriptor);
                        // 更新状态和缓存集合
                        if ("d".equals(op)){
                            //对配置表进行了删除、将缓存和状态中删除
                            broadcastStateMap.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else {
                            //对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
                            broadcastStateMap.put(sourceTable,tableProcessDim);
                            configMap.put(sourceTable,tableProcessDim);
                        }
                    }


                }).print();
        // TODO 5.提交作业
        env.execute("flink-demo-begin-dimApplication");
    }
}
