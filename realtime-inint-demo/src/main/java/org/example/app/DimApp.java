package org.example.app;


import org.example.base.BaseAppSource;
import org.example.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseAppSource {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10002,3,"dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        env.fromElements("s").print();

    }
}
