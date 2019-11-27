package com.lin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaMsgUtil {

	/**
	 * 注:目前每个topic下只有一个分区，后期可改造成根据key，可根据分区策略存放消息
	 * 
	 * @param topic
	 * @param msg
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void sendMsg(String topic, String msg) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaPropertiesUtils.getProperty("kafka.bootstrap.servers"));
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		//producer.send(new ProducerRecord<String, String>(topic, key, msg));
		producer.send(new ProducerRecord<String, String>(topic, msg));
		producer.close();
	}

	/**
	 * 注:一个group中有多个消费者时，只能一个消费者消费同一个topic下的同一分区消息，目前版本一个消费者必须不同的groupId
	 * 
	 * @param topic
	 * @param groupId
	 * @throws Exception
	 */
	public static void getMsg(String topic, String groupId) throws Exception {
		Properties props = new Properties();
		// zookeeper 配置
		props.put("zookeeper.connect", KafkaPropertiesUtils.getProperty("zookeeper.connect"));
		// group 代表一个消费组
		props.put("group.id", groupId);
		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
				keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext())
			System.out.println(it.next().message());
	}

	public static void main(String[] args) throws Exception {
		//发送消息
		KafkaMsgUtil.sendMsg("usd","test");
		
		//接收消息
		KafkaMsgUtil.getMsg("usd","sms");
		
	}

}
