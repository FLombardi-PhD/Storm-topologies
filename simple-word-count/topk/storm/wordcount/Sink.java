package storm.wordcount;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Sink extends BaseRichBolt {

	private static final long serialVersionUID = 3465275538452860232L;
	
	private final int K;
	
	private final String sinkFolder;
	
	private final int secondsToSink;
	
	private OutputCollector collector;
	
	private Logger logger;
	
	private String sinkFilename;
	
	private Map<String, Long> counterMap;
	
	public Sink(int K, String sinkFolder, int secondsToSink) {
		this.K = K;
		this.sinkFolder = sinkFolder;
		this.secondsToSink = secondsToSink;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(Sink.class);
		this.collector = collector;
		counterMap = new HashMap<String, Long>();
		sinkFilename = new File(sinkFolder, context.getStormId() + ".sink").getAbsolutePath();
		logger.info("Sink task ready to write to file " + sinkFilename);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void execute(Tuple input) {
		if (Utils.isTickTuple(input)) {
			writeCountersToSink();
		} else {
			String word = input.getString(0);
			Long counter = input.getLong(1);
			counterMap.put(word, counter);
			collector.ack(input);
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, secondsToSink);
		return conf;
	}

	private void writeCountersToSink() {
		try {
			String topK[] = Utils.getTopK(counterMap, K);
			Map<String, Long> newCounterMap = new HashMap<String, Long>();
			PrintStream sinkWriter = new PrintStream(sinkFilename);
			for (String word : topK)
				if (word != null) {
					Long counter = counterMap.get(word);
					sinkWriter.println(word + ": " + counter);
					newCounterMap.put(word, counter);
				}
			sinkWriter.close();
			logger.info("Counters written to file, " + (counterMap.size() - K) + " entries garbage collected");
			counterMap = newCounterMap;
		} catch (Exception e) {
			logger.error("Error writing counters to sink", e);
		}
	}
}

