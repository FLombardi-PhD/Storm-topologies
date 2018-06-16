/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import java.util.ArrayList;
import java.util.List;

import midlab.storm.scheduler.SchedulerHook;
import midlab.storm.scheduler.SchedulerMetricsConsumer;

import org.apache.log4j.Logger;

import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.StopWordFilter;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.bolt.WordGenerator;
import storm.starter.spout.JMSTweetReader;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.beust.jcommander.JCommander;
// import backtype.storm.testing.TestWordSpout;
// import storm.starter.spout.SimpleTweetReader;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 * 
 * DONE change the spout so that it emits tweets from file
 * DONE add a bolt after the spout that receives tweets and emits the list of words in the body of the tweets
 * DONE add an additional bolt after the previous one that simply filters out stop words
 * TODO check acks
 */
public class TwitterRollingTopWords {

  private static final Logger logger = Logger.getLogger(TwitterRollingTopWords.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  // private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;
  private final Parameters parameters;

  public TwitterRollingTopWords(String topologyName, Parameters parameters) throws InterruptedException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    this.parameters = parameters;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private Config createTopologyConfiguration() {
    Config conf = new Config();
    
    conf.setDebug(parameters.debug);
    
    conf.setNumWorkers(parameters.workerCount);
    
    conf.setStatsSampleRate(1.0);
    
    List<String> hooksList= new ArrayList<String>();
    hooksList.add(SchedulerHook.class.getName());
    // hooksList.add(TupleSizeProfilerHook.class.getName());
    conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hooksList);
    
    conf.registerMetricsConsumer(SchedulerMetricsConsumer.class, 1);
    
    conf.setNumAckers(parameters.ackerCount);
    
    // conf.put("midlab.scheduler.time.bucket.number", "6");
    // conf.put("midlab.scheduler.time.bucket.size", "10");
    
    return conf;
  }
  
  private static final String TWEET_READER_ID = "tweetReader";
  private static final String WORD_GENERATOR_ID = "wordGenerator";
  private static final String STOP_WORD_FILTER_ID = "stopWordFilter";
  private static final String COUNTER_ID = "counter";
  private static final String INTERMEDIATE_RANKER_ID = "intermediateRanker";
  private static final String TOTAL_RANKER_ID = "finalRanker";

  private void wireTopology() throws InterruptedException {
	  
	  /*
	   * for now, set num tasks = num executors
	   */
	  parameters.tweetReaderTaskCount = parameters.tweetReaderExecutorCount;
	  parameters.wordGeneratorTaskCount = parameters.wordGeneratorExecutorCount;
	  parameters.stopWordFilterTaskCount = parameters.stopWordFilterExecutorCount;
	  parameters.counterTaskCount = parameters.counterExecutorCount;
	  parameters.intermediateRankerTaskCount = parameters.intermediateRankerExecutorCount;
	  
	  // builder.setSpout(TWEET_READER_ID, new SimpleTweetReader(parameters.dataset), 1).setNumTasks(1);
	  builder.setSpout(TWEET_READER_ID, 
			  new JMSTweetReader(parameters.jmsServer, parameters.tweetReplicationFactor, parameters.ackerCount > 0, parameters.logThroughput, parameters.logLatency), 
			  parameters.tweetReaderExecutorCount).
	  	setNumTasks(parameters.tweetReaderTaskCount);
	  
	  builder.setBolt(WORD_GENERATOR_ID, new WordGenerator(), parameters.wordGeneratorExecutorCount).
	  	setNumTasks(parameters.wordGeneratorTaskCount).
	  	shuffleGrouping(TWEET_READER_ID);
	  
	  builder.setBolt(STOP_WORD_FILTER_ID, new StopWordFilter(), parameters.stopWordFilterExecutorCount).
    	setNumTasks(parameters.stopWordFilterTaskCount).
    	shuffleGrouping(WORD_GENERATOR_ID);
	  
	  builder.setBolt(COUNTER_ID, new RollingCountBolt(parameters.windowLength, parameters.slidingLength), parameters.counterExecutorCount).
    	setNumTasks(parameters.counterTaskCount).
    	fieldsGrouping(STOP_WORD_FILTER_ID, new Fields("word"));
	  
	  builder.setBolt(INTERMEDIATE_RANKER_ID, new IntermediateRankingsBolt(parameters.topN), parameters.intermediateRankerExecutorCount).
    	setNumTasks(parameters.intermediateRankerTaskCount).
    	fieldsGrouping(COUNTER_ID, new Fields("obj"));
	  
	  builder.setBolt(TOTAL_RANKER_ID, new TotalRankingsBolt(parameters.topN)).globalGrouping(INTERMEDIATE_RANKER_ID);
  }

  public void runLocally() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  /**
   * Submits (runs) the topology.
   *
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
	Parameters parameters = new Parameters();
	new JCommander(parameters, args);
	
	TwitterRollingTopWords rtw = new TwitterRollingTopWords("twitter-top-words", parameters);
	if (parameters.local) {
	  logger.info("Running in local mode");
	  rtw.runLocally();
	}
	else {
	  logger.info("Running in remote (cluster) mode");
	  rtw.runRemotely();
	}
  }
}
