package org.example.app;


import org.example.base.BaseAppSource;
import org.example.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DimApp extends BaseAppSource {
    public static void main(String[] args) throws Exception {
        DimApp dimApp = new DimApp();
        dimApp.start(10002,3,"dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        env.fromElements("s").print();

    }
}
