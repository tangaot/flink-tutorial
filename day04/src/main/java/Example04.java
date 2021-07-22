import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

// WaterMark Demo
public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102",9999)
                .map(
                        new MapFunction<String, Tuple2<String,Long>>() {
                            @Override
                            public Tuple2<String, Long> map(String value) throws Exception {
                                String[] arr = value.split(" ");
                                return Tuple2.of(arr[0], Long.parseLong(arr[1]) );
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
                )
                .keyBy( r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long windowStart = context.window().getStart();
                        long windowEnd   = context.window().getEnd();
                        long count       = elements.spliterator().getExactSizeIfKnown(); // 迭代器里面共多少条元素
                        out.collect("用户：" + key + " 在窗口" +
                                "" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                                "中的pv次数是：" + count);
                    }
                })
                .print();

        env.execute();
    }
}
