/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.illecker.sentistorm;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import at.illecker.sentistorm.bolt.FeatureGenerationBolt;
import at.illecker.sentistorm.bolt.IntermediateRankingsBolt;
import at.illecker.sentistorm.bolt.POSTaggerBolt;
import at.illecker.sentistorm.bolt.PreprocessorBolt;
import at.illecker.sentistorm.bolt.RollingCountBolt;
import at.illecker.sentistorm.bolt.SVMBolt;
import at.illecker.sentistorm.bolt.StopWordFilter;
import at.illecker.sentistorm.bolt.TokenizerBolt;
import at.illecker.sentistorm.bolt.TotalRankingsBolt;
import at.illecker.sentistorm.bolt.WordGenerator;
import at.illecker.sentistorm.commons.Configuration;
import at.illecker.sentistorm.commons.util.io.kyro.TaggedTokenSerializer;
import at.illecker.sentistorm.spout.SentiJMSTweetReader;
import at.illecker.sentistorm.tools.RankableObjectWithFields;
import at.illecker.sentistorm.tools.Rankings;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cmu.arktweetnlp.Tagger.TaggedToken;
import midlab.storm.scheduler.SchedulerHook;
import midlab.storm.scheduler.SchedulerMetricsConsumer;

import com.beust.jcommander.JCommander;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.TreeMapSerializer;


public class SentiRollingTopology {

	private static final Logger logger = Logger.getLogger(SentiRollingTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;
	private final Parameters parameters;

	private static final int DEFAULT_TIMEOUT = 30;  // seconds
	private int timeout = DEFAULT_TIMEOUT;          // seconds

	private boolean ranker;
	
	public SentiRollingTopology(String topologyName, Parameters parameters, int timeout) throws InterruptedException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		this.parameters = parameters;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		this.timeout = timeout;
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

		conf.setMessageTimeoutSecs(timeout);

		/** recommended configuration **/
		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);

		//conf.setMaxSpoutPending(1);

		// conf.put("midlab.scheduler.time.bucket.number", "6");
		// conf.put("midlab.scheduler.time.bucket.size", "10");
		
		//conf.registerSerialization(Rankings.class);
		
		return conf;
	}


	private static final String TWEET_READER_ID = SentiJMSTweetReader.ID;
	private static final String TOKENIZER_ID = TokenizerBolt.ID;
	private static final String PREPROCESSOR_ID = PreprocessorBolt.ID;
	private static final String POS_TAGGER_ID = POSTaggerBolt.ID;
	private static final String FEATURE_GENERATION_ID = FeatureGenerationBolt.ID;
	private static final String SVM_ID = SVMBolt.ID;

	private static final String WORD_GENERATOR_ID = "wordGenerator";
	private static final String STOP_WORD_FILTER_ID = "stopWordFilter";
	private static final String COUNTER_ID = "counter";
	
	private static final String INTERMEDIATE_RANKER_ID = "intermediateRanker";
	private static final String TOTAL_RANKER_ID = "finalRanker";
		
	private void wireTopology() throws InterruptedException {

		/* default task count values
		parameters.tokenizerTaskCount = 4;
		parameters.preprocessorTaskCount = 4;
		parameters.posTaggerTaskCount = 4;
		parameters.featureGenerationTaskCount = 4;
		parameters.svmTaskCount = 4;
		 */

		ranker = true;
		if (parameters.ranker == 0)
			ranker = false;
		
		// check that executor <= task for senti components
		if (parameters.tweetReaderExecutorCount > parameters.tweetReaderTaskCount)
			parameters.tweetReaderTaskCount = parameters.tweetReaderExecutorCount;

		if (parameters.tokenizerExecutorCount > parameters.tokenizerTaskCount)
			parameters.tokenizerTaskCount = parameters.tokenizerExecutorCount;

		if (parameters.preprocessorExecutorCount > parameters.preprocessorTaskCount)
			parameters.preprocessorTaskCount = parameters.preprocessorExecutorCount;

		if (parameters.posTaggerExecutorCount > parameters.posTaggerTaskCount)
			parameters.posTaggerTaskCount = parameters.posTaggerExecutorCount;

		if (parameters.featureGenerationExecutorCount > parameters.featureGenerationTaskCount)
			parameters.featureGenerationTaskCount = parameters.featureGenerationExecutorCount;

		if (parameters.svmExecutorCount > parameters.svmTaskCount)
			parameters.svmTaskCount = parameters.svmExecutorCount;

		// check that executor <= task for rolling components
		if (parameters.wordGeneratorExecutorCount > parameters.wordGeneratorTaskCount)
			parameters.wordGeneratorTaskCount = parameters.wordGeneratorExecutorCount;

		if (parameters.stopWordFilterExecutorCount > parameters.stopWordFilterTaskCount)
			parameters.stopWordFilterTaskCount = parameters.stopWordFilterExecutorCount;

		if (parameters.counterExecutorCount > parameters.counterTaskCount)
			parameters.counterTaskCount = parameters.counterExecutorCount;
		
		if (ranker) {
			if (parameters.intermediateRankerExecutorCount > parameters.intermediateRankerTaskCount)
				parameters.intermediateRankerTaskCount = parameters.intermediateRankerExecutorCount;
		
			if (parameters.finalRankerExecutorCount > parameters.finalRankerTaskCount)
				parameters.finalRankerTaskCount = parameters.finalRankerExecutorCount;
		}
		
		builder.setSpout(TWEET_READER_ID, 
				new SentiJMSTweetReader(parameters.jmsServer, parameters.tweetReplicationFactor, parameters.ackerCount > 0, parameters.logThroughput, parameters.logLatency, parameters.logFail, timeout, parameters.second_update, parameters.rate, parameters.ratio_stream), 
				parameters.tweetReaderExecutorCount).
		setNumTasks(parameters.tweetReaderTaskCount);

		// Setting bolt for SentiStorm topology
		builder.setBolt(TOKENIZER_ID, new TokenizerBolt(), parameters.tokenizerExecutorCount).
		setNumTasks(parameters.tokenizerTaskCount).
		shuffleGrouping(TWEET_READER_ID, "senti-stream");

		builder.setBolt(PREPROCESSOR_ID, new PreprocessorBolt(), parameters.preprocessorExecutorCount).
		setNumTasks(parameters.preprocessorTaskCount).
		shuffleGrouping(TOKENIZER_ID);

		builder.setBolt(POS_TAGGER_ID, new POSTaggerBolt(), parameters.posTaggerExecutorCount).
		setNumTasks(parameters.posTaggerTaskCount).
		shuffleGrouping(PREPROCESSOR_ID);

		builder.setBolt(FEATURE_GENERATION_ID, new FeatureGenerationBolt(), parameters.featureGenerationExecutorCount).
		setNumTasks(parameters.featureGenerationTaskCount).
		shuffleGrouping(POS_TAGGER_ID);

		builder.setBolt(SVM_ID, new SVMBolt(), parameters.svmExecutorCount).
		setNumTasks(parameters.svmTaskCount).
		shuffleGrouping(FEATURE_GENERATION_ID);

		// Setting bolt for Rolling-Top-Words topology
		builder.setBolt(WORD_GENERATOR_ID, new WordGenerator(), parameters.wordGeneratorExecutorCount).
		setNumTasks(parameters.wordGeneratorTaskCount).
		shuffleGrouping(TWEET_READER_ID, "rolling-stream");

		builder.setBolt(STOP_WORD_FILTER_ID, new StopWordFilter(), parameters.stopWordFilterExecutorCount).
		setNumTasks(parameters.stopWordFilterTaskCount).
		shuffleGrouping(WORD_GENERATOR_ID);

		builder.setBolt(COUNTER_ID, new RollingCountBolt(parameters.windowLength, parameters.slidingLength, ranker), parameters.counterExecutorCount).
		setNumTasks(parameters.counterTaskCount).
		fieldsGrouping(STOP_WORD_FILTER_ID, new Fields("word"));

		if (ranker) {
			builder.setBolt(INTERMEDIATE_RANKER_ID, new IntermediateRankingsBolt(parameters.topN), parameters.intermediateRankerExecutorCount).
			setNumTasks(parameters.intermediateRankerTaskCount).
			fieldsGrouping(COUNTER_ID, new Fields("obj"));

			builder.setBolt(TOTAL_RANKER_ID, new TotalRankingsBolt(parameters.topN), parameters.finalRankerExecutorCount).
			setNumTasks(parameters.finalRankerTaskCount).
			globalGrouping(INTERMEDIATE_RANKER_ID);
		}
		
		// adding further configuration from original SentiStorm
		topologyConfig.put(TokenizerBolt.CONF_LOGGING,
				Configuration.get("sentistorm.bolt.tokenizer.logging", false));
		topologyConfig.put(PreprocessorBolt.CONF_LOGGING,
				Configuration.get("sentistorm.bolt.preprocessor.logging", false));
		topologyConfig.put(POSTaggerBolt.CONF_LOGGING,
				Configuration.get("sentistorm.bolt.postagger.logging", false));
		topologyConfig.put(POSTaggerBolt.CONF_MODEL,
				Configuration.get("sentistorm.bolt.postagger.model"));
		topologyConfig.put(FeatureGenerationBolt.CONF_LOGGING,
				Configuration.get("sentistorm.bolt.featuregeneration.logging", false));
		topologyConfig.put(SVMBolt.CONF_LOGGING,
				Configuration.get("sentistorm.bolt.svm.logging", false));


		topologyConfig.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
		topologyConfig.registerSerialization(TaggedToken.class, TaggedTokenSerializer.class);
		topologyConfig.registerSerialization(TreeMap.class, TreeMapSerializer.class);
		
		topologyConfig.registerSerialization(Rankings.class);
		topologyConfig.registerSerialization(RankableObjectWithFields.class);
		//topologyConfig.registerSerialization(com.google.common.collect.ImmutableList.class);

	}

	public void runLocally() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	}

	public static void main(String[] args) throws Exception {
		Parameters parameters = new Parameters();
		new JCommander(parameters, args);

		int timeout = 30;

		SentiRollingTopology sst = new SentiRollingTopology("senti-rolling", parameters, timeout);

		if (parameters.local) {
			logger.info("Running in local mode");
			sst.runLocally();
		}
		else {
			logger.info("Running in remote (cluster) mode");
			sst.runRemotely();
		}
	}

}
