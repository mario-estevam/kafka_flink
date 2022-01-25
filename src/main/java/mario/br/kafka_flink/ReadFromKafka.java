package mario.br.kafka_flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<>(parameterTool.getRequired("topic"),new SimpleStringSchema(),parameterTool.getProperties()));
		messageStream.map(new MapFunction<String, String>(){
			private static final long serialVersionUID = -6867736771747690202L;
			@Override
			public String map(String value) throws Exception{
				return "Kafka e flink sainda:"+value;
			}
		}).rebalance().print();
		env.execute();
	}
}
