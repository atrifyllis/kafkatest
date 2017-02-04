package gr.alx.gr.alx.kafka;

import info.batey.kafka.unit.KafkaUnitRule;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * Created by alx on 2/4/2017.
 */
public class TestKafka {

	private static final String TEST_TOPIC = "test-topic";
	@Rule
	public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(5000, 5001);

	@Before
	public void setUp() {
		kafkaUnitRule.getKafkaUnit().createTopic(TEST_TOPIC, 3);
	}

	@Test
	public void test1() throws InterruptedException {

		produce();

		consume();

	}

	void produce() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:5001");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++)
			producer.send(new ProducerRecord<String, String>(TEST_TOPIC, Integer.toString(i), Integer.toString(i)));

		producer.close();
	}

	void consume() {
		System.out.println("start consuming");
		System.out.println("...");
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:5001");
		props.put("group.id", "test");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(TEST_TOPIC));
		List<ConsumerRecord<String, String>> all = new ArrayList<>();
		boolean run = true;
		while (run) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			records.forEach(r -> all.add(r));
			all.sort(Comparator.comparing(c -> Integer.parseInt(c.key())));
			all.forEach(record -> System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
			if (all.size() == 100) run = false;
		}

	}
}
