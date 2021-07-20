import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

// Integer increases consecutively for 1s
public class Example6 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (true) {
                            ctx.collect(random.nextInt(100));
                            Thread.sleep(300L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                }).keyBy(r -> true)
                .process(new IntIncreaseAlert())
                .print();

        env.execute();
    }

    public static class IntIncreaseAlert extends KeyedProcessFunction<Boolean, Integer, String> {
        private ValueState<Integer> prevInt;
        private ValueState<Long> timeStamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            prevInt = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("prev-integer", Types.INT));
            timeStamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer",Types.LONG));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
            Integer tempInt = null;
            out.collect(""+value);
            if (prevInt.value() != null) {
                tempInt = prevInt.value();
            }
            prevInt.update(value);

            Long tempTs = null;
            if ( timeStamp.value() != null ){
                tempTs = timeStamp.value();
            }

            if ( tempInt==null || value < tempInt  ){
                if ( tempTs != null){
                    ctx.timerService().deleteProcessingTimeTimer(tempTs);
                    timeStamp.clear();
                }
            } else if (value > tempInt && tempTs == null) {

                    long oneSecLater = ctx.timerService().currentProcessingTime() + 1000L;
                    ctx.timerService().registerProcessingTimeTimer(oneSecLater);
                    timeStamp.update(oneSecLater);

            }



        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("整数连续1s上升了！");
            timeStamp.clear();
        }
    }
}
