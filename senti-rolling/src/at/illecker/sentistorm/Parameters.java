package at.illecker.sentistorm;

import com.beust.jcommander.Parameter;

public class Parameters {

	@Parameter(names = { "-wc", "-worker-count" }, description = "Number of workers to use (default: 1)")
	public int workerCount = 1;

	@Parameter(names = {"-ac", "-acker-count"}, description = "Number of ackers to use (default: 1)")
	public int ackerCount = 1;

	@Parameter(names = { "-l", "-local" }, arity = 1, description = "If true the topology is executed locally, if false it is submitted to cluster (default: false)")
	public boolean local = false;

	@Parameter(names = { "-d", "-debug" }, arity = 1, description = "Debug flag (default: true)")
	public boolean debug = true;

	@Parameter(names = { "-lt", "-log-throughput" }, arity = 1, description = "Flag to control whether to log throughput info to file (default: false)")
	public boolean logThroughput = false;

	@Parameter(names = { "-ll", "-log-latency" }, arity = 1, description = "Flag to control whether to log latency values to file (default: false)")
	public boolean logLatency = false;

	@Parameter(names = { "-lf", "-log-fail" }, arity = 1, description = "Flag to control whether to log tuple failures to file (default: false)")
	public boolean logFail = false;

	// @Parameter(names = { "-dataset" }, description = "Path of input dataset", required = true)
	// public String dataset;

	@Parameter(names = { "-js", "-jms-server" }, description = "Hostname of JMS server", required = true)
	public String jmsServer;

	@Parameter(names = { "-btc" }, description = "How many tuples per batch? (default: 10)")
	public int batchTupleCount = 10;

	@Parameter(names = { "-bl" }, description = "Batch length, in milliseconds (default: 100)")
	public int batchLength = 100;

	@Parameter(names = { "-topN" }, description = "How many top words? (default: 5)")
	public int topN = 5;

	@Parameter(names = { "-wl", "-window-length" }, description = "Length of window in seconds (default: 9)")
	public int windowLength = 9;

	@Parameter(names = { "-sl", "-sliding-length" }, description = "Length of sliding in seconds (default: 3)")
	public int slidingLength = 3;

	@Parameter(names = { "-trf", "-tweet-replication-factor" }, description = "How many tweets emit from the spout for each tweet received from the JMS queue? (default: 1)")
	public int tweetReplicationFactor = 1;

	@Parameter(names = { "-trec", "-tweet-reader-executor-count" }, description = "Number of executors for tweet-reader spout (default: 1)")
	public int tweetReaderExecutorCount = 1;

	@Parameter(names = { "-trtc", "-tweet-reader-task-count" }, description = "Number of tasks for tweet-reader spout (default: 1)")
	public int tweetReaderTaskCount = 1;

	/** 
	 * Parameters for SentiStorm topology
	 */
	
	@Parameter(names = { "-tokec", "-tokenizer-executor-count" }, description = "Number of executors for tokenizer bolt (default: 1)")
	public int tokenizerExecutorCount = 1;

	@Parameter(names = { "-toktc", "-tokenizer-task-count" }, description = "Number of tasks for tokenizer bolt (default: 1)")
	public int tokenizerTaskCount = 1;

	@Parameter(names = { "-ppec", "-preprocessor-executor-count" }, description = "Number of executors for preprocessor bolt (default: 1)")
	public int preprocessorExecutorCount = 1;

	@Parameter(names = { "-pptc", "-preprocessor-task-count" }, description = "Number of tasks for preprocessor bolt (default: 1)")
	public int preprocessorTaskCount = 1;

	@Parameter(names = { "-ptec", "-postagger-executor-count" }, description = "Number of executors for pos-tagger bolt (default: 1)")
	public int posTaggerExecutorCount = 1;

	@Parameter(names = { "-pttc", "-postagger-task-count" }, description = "Number of tasks for pos-tagger bolt (default: 1)")
	public int posTaggerTaskCount = 1;

	@Parameter(names = { "-fgec", "-feature-generation-executor-count" }, description = "Number of executors for feature-generation bolt (default: 1)")
	public int featureGenerationExecutorCount = 1;

	@Parameter(names = { "-fgtc", "-feature-generation-task-count" }, description = "Number of tasks for feature-generation bolt (default: 1)")
	public int featureGenerationTaskCount = 1;

	@Parameter(names = { "-svmec", "-svm-executor-count" }, description = "Number of executors for svm bolt (default: 1)")
	public int svmExecutorCount = 1;

	@Parameter(names = { "-svmtc", "-svm-task-count" }, description = "Number of tasks for svm bolt (default: 1)")
	public int svmTaskCount = 1;

	
	/** 
	 * Parameters for Rolling-Top-Words topology
	 */
		
	@Parameter(names = { "-wgec", "-word-generator-executor-count" }, description = "Number of executors for word-generator bolt (default: 1)")
	public int wordGeneratorExecutorCount = 1;
	
	@Parameter(names = { "-wgtc", "-word-generator-task-count" }, description = "Number of tasks for word-generator bolt (default: 1)")
	public int wordGeneratorTaskCount = 1;
	
	@Parameter(names = { "-swfec", "-stop-word-filter-executor-count" }, description = "Number of executors for stop-word-filter bolt (default: 1)")
	public int stopWordFilterExecutorCount = 1;
	
	@Parameter(names = { "-swftc", "-stop-word-filter-task-count" }, description = "Number of tasks for stop-word-filter bolt (default: 1)")
	public int stopWordFilterTaskCount = 1;
	
	@Parameter(names = { "-cec", "-counter-executor-count" }, description = "Number of executors for counter bolt (default: 1)")
	public int counterExecutorCount = 1;
	
	@Parameter(names = { "-ctc", "-counter-task-count" }, description = "Number of tasks for counter bolt (default: 1)")
	public int counterTaskCount = 1;
	
	@Parameter(names = { "-irec", "-intermediate-ranker-executor-count" }, description = "Number of executors for intermediate ranker bolt (default: 1)")
	public int intermediateRankerExecutorCount = 1;
	
	@Parameter(names = { "-irtc", "-intermediate-ranker-task-count" }, description = "Number of tasks for intermediate ranker bolt (default: 1)")
	public int intermediateRankerTaskCount = 1;
	
	/** FINAL **/
	@Parameter(names = { "-frec", "-final-ranker-executor-count" }, description = "Number of executors for final ranker bolt (default: 1)")
	public int finalRankerExecutorCount = 1;
	
	@Parameter(names = { "-frtc", "-final-ranker-task-count" }, description = "Number of tasks for final ranker bolt (default: 1)")
	public int finalRankerTaskCount = 1;
	
	@Parameter(names = { "-r", "-ranker" }, description = "If enable intermediate and final ranker (default: 1 i.e. true)")
	public int ranker = 1;
	
	@Parameter(names = { "-su", "-second-update" }, description = "Interval in seconds for aggregation of tuples toward the second topology (default: 1)")
	public int second_update = 1;
	
	@Parameter(names = { "-rate"}, description = "Rate for the second topology (default: 100)")
	public int rate = 100;
	
	@Parameter(names = { "-ratio"}, description = "Ratio percentage between two topologies (default: 2)")
	public int ratio_stream = 2;
}
