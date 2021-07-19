import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

//Filter
public class Example3 {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> inputStream = env.addSource(new ClickSource());

        //Method 1
        //inputStream.filter(r -> r.user.equals("Mary")).print();

        //Method 2
        /*inputStream.filter(
                new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.user.equals("Mary");
                    }
                }
        ).print();*/

        //Method 3
        //inputStream.filter(new MyFilter()).print();

        //Method 4
        inputStream.flatMap(new FlatMapFunction<Event, Event>() {
            @Override
            public void flatMap(Event value, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")) {
                    out.collect(value);
                }
            }
        }).print();


        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }

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
