import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;


// 独立访客数量
// Bloom Filter
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("req/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],arr[1],arr[2],arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> true)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            long count = elements.iterator().next();
            out.collect("窗口" + windowStart + "~" + windowEnd + "的uv统计值是：" + count);
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Tuple2<Long, BloomFilter<String>>, Long> {
        @Override
        public Tuple2<Long, BloomFilter<String>> createAccumulator() {
            return Tuple2.of(0L, BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100000, 0.01));
        }

        @Override
        public Tuple2<Long, BloomFilter<String>> add(UserBehavior value, Tuple2<Long, BloomFilter<String>> accumulator) {
            if (!accumulator.f1.mightContain(value.userId)) {
                accumulator.f1.put(value.userId); // 将对应位置为1
                accumulator.f0 += 1L;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Long, BloomFilter<String>> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple2<Long, BloomFilter<String>> merge(Tuple2<Long, BloomFilter<String>> a, Tuple2<Long, BloomFilter<String>> b) {
            return null;
        }
    }


    public static class UserBehavior {
        public String userId;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
