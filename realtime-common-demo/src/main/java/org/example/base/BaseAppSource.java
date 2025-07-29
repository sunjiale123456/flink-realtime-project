package org.example.base;

import org.example.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAppSource {

    public void start (int port,int parallelism ,String ckAndGroupId,String topic) throws Exception {
        // TODO 1.基本环境准备
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        // TODO 2.检查点相关设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

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
        env.getCheckpointConfig().setCheckpointStorage(Constant.HDFS_DIR+ckAndGroupId);
        // 7.设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","sunjiale");

        // TODO 3.获取kafka业务数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(ckAndGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka Source");

        // TODO 4.处理逻辑
        handle(env,kafkaSource);
        // TODO 5.提交作业
        env.execute("flink-demo-begin-"+ckAndGroupId);

    }
    public abstract void handle (StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource);
}
