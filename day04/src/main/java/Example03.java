import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

// 使用KeyedProcessFunction模拟5秒的滚动窗口，模拟的是增量聚合函数和全窗口聚合函数结合使用的情况
public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.user)
                .process(new FakeWindow())
                .print();

        env.execute();
    }

    public static class FakeWindow extends KeyedProcessFunction<String, Event, String> {
        private MapState<Long, Integer> windowState;
        private Long windowSize = 5000L;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            windowState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Integer>("windowStart-pvNumber",Types.LONG,Types.INT)
            );
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 计算当前元素所属的窗口的开始时间
            long currTime = ctx.timerService().currentProcessingTime();
            long windowStart = currTime - currTime % windowSize;
            long windowEnd   = windowStart + windowSize;

            if (windowState.contains(windowStart)) {
                windowState.put(windowStart, windowState.get(windowStart) + 1);
            } else {
                windowState.put(windowStart, 1);
            }

            ctx.timerService().registerProcessingTimeTimer(windowEnd - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            long windowEnd = timestamp + 1L;
            long windowStart = windowEnd - windowSize;
            long count = windowState.get(windowStart);
            out.collect("用户：" + ctx.getCurrentKey() + " 在窗口" +
                    "" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                    "中的pv次数是：" + count);
            windowState.remove(windowStart);
        }
    }


    // SourceFunction并行度只能为1
    // 自定义并行化版本的数据源，需要使用ParallelSourceFunction
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random();
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                // collect方法，向下游发送数据
                ctx.collect(
                        new Event(
                                userArr[random.nextInt(userArr.length)],
                                urlArr[random.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
