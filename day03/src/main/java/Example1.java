import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 富函数
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1,2,3)
                .map(new RichMapFunction<Integer, Integer>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("生命周期开始");
                        System.out.println("当前子任务的索引是：" + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("生命周期关闭");
                    }
                }).print();
        env.execute();
    }
}
