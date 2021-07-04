package com.training.kafka.twittter;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import com.twitter.hbc.core.Client;

public class PublishTweets {
	
	Logger logger = LoggerFactory.getLogger(PublishTweets.class);
	private TwitterClient twitterClient = new TwitterClient() ;
	private Producer producer = new Producer();
	private String topic = "twitter_tweets";
	private int tweetsToRead = 10;
	
	public void run() throws InterruptedException {
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		//create twitter client
		final Client client = twitterClient.getTwitterClient(msgQueue, Arrays.asList("covid"));
		client.connect();
		
		//create kafka producer
		KafkaProducer<String, String> kafkaProducer = producer.getKafkaProducer();
		
		//loop through tweets and send to kafka
		int tweetsProcessed = 0;
		while (!client.isDone()) {
		   String msg = msgQueue.take();
		   if(msg != null) {
			  tweetsProcessed += 1;
			  String key = ExtractIdFromTweets(msg);
			  logger.info("key::"+key);
			  if(tweetsProcessed <= tweetsToRead) {
						  ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, msg);
						  kafkaProducer.send(record, new Callback() {
							
							@Override
							public void onCompletion(RecordMetadata metadata, Exception exception) {
								if(exception != null) {
									logger.error(exception.getMessage());
								}
								
							}
						});
						  logger.info(msg);
					}
		    else {
			  
			  logger.info("Stopping application after {} tweets",tweetsProcessed);	
			  client.stop();
			 
			   kafkaProducer.flush();
				kafkaProducer.close();
			   
		    }
		   }	  
		}
	    
	}

	private String ExtractIdFromTweets(String tweets) {
        JsonParser jsonParser = new JsonParser();
        String id = jsonParser.parse(tweets)
		            .getAsJsonObject()
		            .get("id_str")
		            .getAsString();
		return id;
	}

}
