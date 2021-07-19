package day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Aggregate rolling sum of data stream
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer, Integer>> inputStream = env.fromElements(
                Tuple2.of(1, 2),
                Tuple2.of(1, 3),
                Tuple2.of(2, 6)
        );
        KeyedStream<Tuple2<Integer, Integer>, Integer> groupedStream = inputStream.keyBy(r -> r.f0);

        //Aggregate rolling sum of data stream
        groupedStream.sum(1).print();

        //find max of data stream
        groupedStream.reduce(
                new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,Math.max(value1.f1, value2.f1));
                    }
                }
        ).print();
        env.execute();
    }
}
