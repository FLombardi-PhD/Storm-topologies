package storm.starter;

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

}
