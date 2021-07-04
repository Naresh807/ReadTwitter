package com.training.kafka.twittter;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterClient {
	
	private String consumerKey = "O3M17KcKSrEPJ1ri28PrMhodl";
	private String consumerSecret = "KBhLzkWhI4od3Knnrcvf7WVZUb7oWH15zJqPjmRpnXWyj3qtOR";
	private String token = "905985770141261824-ZOzfRPpTSbfTS35IZ21rGCeSOA5iLDK";
	private String secret = "kmwXu3dDEoQqLNufDBcvPTYGbVvNScZCvvzlJ4t0PRr7p";
	
	public Client getTwitterClient(BlockingQueue<String> msgQueue, List<String> trackTerms) {
		
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		hosebirdEndpoint.trackTerms(trackTerms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));


				Client hosebirdClient = builder.build();
				
				return hosebirdClient;
				
	}

}
