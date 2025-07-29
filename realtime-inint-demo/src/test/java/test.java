import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.Map;
import java.util.Set;

public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///F:/Idea/javaProject/chk/DimAppTest/");
        String filePath = "F:\\Idea\\javaProject\\flink-realtime-project\\realtime-inint-demo\\src\\main\\java\\config\\config.txt";
        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineFormat(), // 文本行格式
                new Path(filePath)) // 监控路径
                .monitorContinuously(java.time.Duration.ofSeconds(10)) // 监控间隔
                .build();

        // 2. 创建数据流
        env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(), // 水印策略
                "File Source"
        ).print();
        // 在创建文件源时增加监控间隔参数
        env.execute();
    }
}
