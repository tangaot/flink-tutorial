import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


    }
}
