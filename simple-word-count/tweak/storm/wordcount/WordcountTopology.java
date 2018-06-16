package storm.wordcount;

import com.beust.jcommander.JCommander;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordcountTopology {

	private final Parameters parameters;
	private final Config config;
	private final StormTopology topology;
	
	private static final String DATASET_READER_ID = "dataset_reader";
	private static final String SPLITTER_ID = "splitter";
	private static final String FILTER_ID = "filter";
	private static final String COUNTER_ID = "counter";
	private static final String SINK_ID = "sink";
	
	public WordcountTopology(Parameters parameters) {
		this.parameters = parameters;
		config = createTopologyConfiguration();
		topology = createTopology();
	}
	
	private Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(parameters.debug);
		conf.setNumWorkers(parameters.workerCount);
		conf.setStatsSampleRate(1.0);
		conf.setNumAckers(parameters.ackerCount);
		return conf;
	}
	
	private StormTopology createTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(DATASET_READER_ID, new DatasetReader(parameters.dataset, parameters.inputRate));
		
		builder.setBolt(SPLITTER_ID, new Splitter(), parameters.splitterReplication).shuffleGrouping(DATASET_READER_ID);
		
		builder.setBolt(FILTER_ID, new StopWordFilter(), parameters.filterReplication).shuffleGrouping(SPLITTER_ID);
		
		builder.setBolt(COUNTER_ID, new Counter(parameters.secondsToSendCounters), parameters.counterReplication).fieldsGrouping(FILTER_ID, new Fields("word"));
		
		builder.setBolt(SINK_ID, new Sink(parameters.sinkFolder, parameters.secondsToSink)).shuffleGrouping(COUNTER_ID);
		
		return builder.createTopology();
	}
	
	public void runLocally() {
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology(parameters.topologyName, config, topology);
		try {
			Thread.sleep(parameters.localExecutionLength * 1000);
		} catch (InterruptedException e) {}
		localCluster.killTopology(parameters.topologyName);
		localCluster.shutdown();
	}
	
	public void runRemotely() {
		try {
			StormSubmitter.submitTopologyWithProgressBar(parameters.topologyName, config, topology);
		} catch (Exception e) {
			System.out.println("Error submitting remotely: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Parameters parameters = new Parameters();
		new JCommander(parameters, args);
		
		WordcountTopology wordcountTopology = new WordcountTopology(parameters);
		if (parameters.local)
			wordcountTopology.runLocally();
		else
			wordcountTopology.runRemotely();
	}

}
