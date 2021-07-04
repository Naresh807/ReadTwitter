 package com.training.kafka.twittter;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	
	 private String bootStrapServer = "127.0.0.1:9092";
		
	 public KafkaProducer<String, String> getKafkaProducer(){
		 
		// create Producer properties
		  Properties properties = new Properties();
		  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
       // create safe producer
		  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		  properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		  properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
	  //  high throughput producer (at the expense of a bit of latency and CPU usage)
		  properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		  properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		  properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		  
  
		
		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		return producer;		 
	 }
	 
	
	
}
