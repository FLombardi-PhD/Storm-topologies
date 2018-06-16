package storm.wordcount;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Counter extends BaseRichBolt {

	private static final long serialVersionUID = 1421036283087292370L;
	
	private final int secondsToSendCounters;
	
	private final int K;
	
	private Logger logger;
	
	private OutputCollector collector;
	
	private Map<String, Long> counterMap;
	
	private Set<String> updatedCounterSet;
	
	public Counter(int K, int secondsToSendCounters) {
		this.K = K;
		this.secondsToSendCounters = secondsToSendCounters;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(Counter.class);
		counterMap = new HashMap<String, Long>();
		updatedCounterSet = new HashSet<String>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "counter"));
	}

	@Override
	public void execute(Tuple input) {
		if (Utils.isTickTuple(input)) {
			sendUpdatedCounters(input);
		} else {
			String word = input.getString(0);
			long counter = incrementCounter(word);
			logger.debug(word + ": " + counter);
		}
		collector.ack(input);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, secondsToSendCounters);
		return conf;
	}

	private long incrementCounter(String word) {
		Long currentCounter = counterMap.get(word);
		if (currentCounter == null)
			currentCounter = Long.valueOf(0);
		currentCounter++;
		counterMap.put(word, currentCounter);
		updatedCounterSet.add(word);
		return currentCounter;
	}
	
	private void sendUpdatedCounters(Tuple tick) {
		String topK[] = Utils.getTopK(counterMap, K);
		for (int i = 0; i < K; i++)
			if (updatedCounterSet.contains(topK[i])) {
				long counter = counterMap.get(topK[i]);
				collector.emit(tick, new Values(topK[i], counter));
			}
		logger.info("" + K + " entries sent out of " + counterMap.size());
	}
}

