package com.tragger;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class streamState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String,Integer>> dataStreamSource = env.fromElements(
                Tuple2.of("a",1),  Tuple2.of("a",2),  Tuple2.of("b",3),
                Tuple2.of("a",1),  Tuple2.of("a",2),  Tuple2.of("b",3));

        MapStateDescriptor<String,bean> metaStateDescriptor = new MapStateDescriptor<String,bean>("metaStateDescriptor", TypeInformation.of(String.class),TypeInformation.of(bean.class)){};
        dataStreamSource.keyBy(tp2 -> tp2.f0).
                process(new keyFunction(metaStateDescriptor))
                .print();
        env.execute();

    }
}
class keyFunction extends KeyedProcessFunction <String, Tuple2<String, Integer>, Tuple2<String, Integer>>{

    private transient MapState<String ,bean> mapSate;

    private MapStateDescriptor<String,bean> metaStateDescriptor ;

    public keyFunction(MapStateDescriptor<String,bean> metaStateDescriptor) {
        this.metaStateDescriptor=metaStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        metaStateDescriptor.enableTimeToLive(ttlConfig);
        mapSate= getRuntimeContext().getMapState(metaStateDescriptor);

    }

    @Override
    public void processElement(Tuple2<String, Integer> tp2, Context context, Collector<Tuple2<String, Integer>> out) throws Exception {
        bean bean = mapSate.get(tp2.f0);
        if (bean==null){
            mapSate.put(tp2.f0,new bean(tp2.f0, tp2.f1));
            out.collect(tp2);
        }else {
            int score = bean.getScore();
            mapSate.put(tp2.f0,new bean(tp2.f0, tp2.f1 + score));
            out.collect(Tuple2.of(tp2.f0,mapSate.get(tp2.f0).getScore()));
        }
        Iterator<Map.Entry<String, com.tragger.bean>> iterator = mapSate.iterator();
        System.out.println("打印-----------");
        while (iterator.hasNext()){
            System.out.println("数据 ："+iterator.next());
        }
    }
}
class bean implements Serializable {
    private final  long serialVersionUID = -174943434646734354L;
    public bean(String id, int score) {
        this.id = id;
        this.score = score;
    }
    private String id ;
    private int score;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "bean{" +
                "id='" + id + '\'' +
                ", score=" + score +
                '}';
    }
}