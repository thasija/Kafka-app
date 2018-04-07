package com.sai.kafka.Kafka_Consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerExample {

	private final static  String TOPIC = "test-topic";
	private final static String BROKERS = "hdi1rvmk01-inc.tw-test.net:6667,hdi1rvmk02-inc.tw-test.net:6667,hdi1rvmk03-inc.tw-test.net:6667";
	
	private static Consumer<Long, String> createConsumer()
	{
	final Properties props = new Properties();
	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            BROKERS);
	props.put(ConsumerConfig.GROUP_ID_CONFIG,"ConsumerExample");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
             LongDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
             StringDeserializer.class.getName());
	
    Consumer<Long, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(TOPIC));
    return consumer;
    
	}
	
	private static void runConsumer()
	{
	    Consumer<Long, String> consumer = createConsumer();
		final int giveUp = 100;   int noRecordsCount = 0;
		
		while(true)
		{
			final ConsumerRecords<Long,String> consumerrecords = consumer.poll(1000);
			System.out.println(consumerrecords.count());
			if(consumerrecords.count()==0)
			{
				noRecordsCount++;
				if (noRecordsCount > giveUp)
						break;
                else 
                	continue;	
			}
			consumerrecords.forEach(record -> {
				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		runConsumer();
	}

}
