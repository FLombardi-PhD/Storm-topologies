package storm.wordcount;

import com.beust.jcommander.Parameter;

public class Parameters {

	@Parameter(names = { "-tn", "-topology-name" }, description = "Name of the topology (default: \"wordcount\")")
	public String topologyName = "wordcount";
	
	@Parameter(names = { "-dataset" }, description = "Path of input dataset", required = true)
	public String dataset;
	
	@Parameter(names = { "-sf", "-sink-folder" }, description = "Path of the folder where sink files are put (default: .)")
	public String sinkFolder = ".";
	
	@Parameter(names = { "-ir", "-input-rate" }, description = "Tuples per second emitted by the spout (default: 10 tuple/s)")
	public int inputRate = 10;
	
	@Parameter(names = { "-K" }, description = "Sink bolt will output the top K words (default: 10 words)")
	public int K = 10;
	
	@Parameter(names = { "-stsc", "-seconds-to-send-counters" }, description = "Counter bolt will send counters to sink once every secondsToSendCounters seconds (default: 2 seconds)")
	public int secondsToSendCounters = 2;
	
	@Parameter(names = { "-sts", "-seconds-to-sink" }, description = "Sink bolt will write counters to file once every secondsToSink seconds (default: 2 seconds)")
	public int secondsToSink = 2;
	
	@Parameter(names = { "-tts", "-tuples-to-sink" }, description = "Sink bolt will write counters to file once every tuplesToSink tuples (default: 100 tuples)")
	public int tuplesToSink = 100;

	@Parameter(names = { "-lel", "-local-execution-length" }, description = "In case of local execution, its length (default: 30 seconds)")
	public int localExecutionLength = 30;
	
	@Parameter(names = { "-d", "-debug" }, arity = 1, description = "Debug flag (default: true)")
	public boolean debug = true;
	
	@Parameter(names = { "-l", "-local" }, arity = 1, description = "If true the topology is executed locally, if false it is submitted to cluster (default: false)")
	public boolean local = false;
	
	@Parameter(names = { "-sr", "-splitter-replication" }, description = "Executor count for Splitter bolt (default: 1)")
	public int splitterReplication = 1;
	
	@Parameter(names = { "-fr", "-filter-replication" }, description = "Executor count for Filter bolt (default: 1)")
	public int filterReplication = 1;
	
	@Parameter(names = { "-cr", "-counter-replication" }, description = "Executor count for Counter bolt (default: 1)")
	public int counterReplication = 1;
	
	@Parameter(names = { "-wc", "-worker-count" }, description = "Number of workers to use (default: 1)")
	public int workerCount = 1;
	
	@Parameter(names = {"-ac", "-acker-count"}, description = "Number of ackers to use (default: 1)")
	public int ackerCount = 1;
	
}
