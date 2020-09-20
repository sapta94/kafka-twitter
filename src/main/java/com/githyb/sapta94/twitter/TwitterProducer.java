package com.githyb.sapta94.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.omg.CORBA.TIMEOUT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import io.github.cdimascio.dotenv.Dotenv;

public class TwitterProducer {

    Dotenv dotenv = Dotenv.configure()
            .directory("/Users/saptarshidey/Documents/kafka-twitter")
            .ignoreIfMalformed()
            .ignoreIfMissing()
            .load();


    public TwitterProducer(){}

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        System.out.println("Setup");
        System.out.println(dotenv.get("consumerKey"));
        // create a twitter client
        Client client = createTwitterClient(msgQueue);

        //Establish connection
        client.connect();
        // create a kafka producer
        System.out.println("connected");
        //loop to send tweets to kafka
        while(!client.isDone()){
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg!=null){
                System.out.println(msg);
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        String consumerKey=dotenv.get("consumerKey");
        String consumerSecret=dotenv.get("consumerSecret");
        String token = dotenv.get("token");
        String tokenSecret = dotenv.get("tokenSecret");

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("ipl");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

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
