import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> 1)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

    public static class MyKeyed extends KeyedProcessFunction<Integer,String,String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // 当前机器时间
            long ts = ctx.timerService().currentProcessingTime();
            out.collect("元素：" + value + " 在" + new Timestamp(ts) + " 到达");
            // 注册一个10秒钟之后的定时器
            long tenSecLater = ts + 10000L;
            out.collect("注册了一个时间在：" + new Timestamp(tenSecLater) + " 的定时器");
            // 注册定时器的语法，注意：注册的是处理时间（机器时间）
            ctx.timerService().registerProcessingTimeTimer(tenSecLater);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！触发时间是：" + new Timestamp(timestamp));
        }
    }
}
