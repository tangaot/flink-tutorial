package kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

//Flink to Kafka
public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        env
                .readTextFile("/req/src/main/resources/UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "user-behavior-1",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
