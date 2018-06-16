package storm.starter.spout;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ConstantRateTweetReader extends BaseRichSpout {
	
	private static final long serialVersionUID = 1522234544518422314L;
	private static final String TWEET = "{ \"body\": [\"@r_formigoni Voi ve la cantate e ve la suonate. Ma quante minchiate dite? Accusi B di essere l'amico di Renzi ma tu sei al governo con Renzi\"]}";
	
	private int tupleRatePerBatch;
	private int batchLength; // ms
	private SpoutOutputCollector collector;
	private Logger logger;
	private String msgIdPrefix;
	private long msgId;
	
	private boolean logThroughput;
	private PrintStream throughputLog;
	
	private boolean logLatency;
	private PrintStream latencyLog;
	private Map<String, Long> latencyMap;
	private Map<String, Long> tsMap;
	
	private boolean useAckers;
	
	public ConstantRateTweetReader(int tupleRatePerBatch, int batchLength, boolean useAckers, boolean logThroughput, boolean logLatency) {
		this.tupleRatePerBatch = tupleRatePerBatch;
		this.batchLength = batchLength;
		this.useAckers = useAckers;
		this.logThroughput = logThroughput;
		this.logLatency = logLatency;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(ConstantRateTweetReader.class);
		msgIdPrefix = "id-" + context.getThisTaskId() + "-" + System.currentTimeMillis() + "-";
		
		StringBuffer sb = new StringBuffer();
		int tupleRate = (int)(tupleRatePerBatch * ((float)1000 / batchLength));
		sb.append("ConstantRateTweetReader spout initialized, tuple rate " + tupleRate + "t/s (batches of " + batchLength + " ms, " + tupleRatePerBatch + " tuples per batch)");
		
		try {
			if (logThroughput) {
				String filename = "throughput." + context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId() + ".csv";
				throughputLog = new PrintStream(filename);
				sb.append(", logging throughput to " + filename);
			}
			
			if (logLatency) {
				String filename = context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId()  +".csv";
				latencyLog = new PrintStream(filename);
				latencyMap = new HashMap<String, Long>();
				tsMap = new HashMap<String, Long>();
				sb.append(", logging latencies to " + filename);
			}
			
			logger.info(sb.toString());
			
		} catch (Exception e) {
			logger.error("Error initializing the spout", e);
		}
	}
	
	private long beginBatch;
	private int tuplesSentInThisBatch;

	@Override
	public void nextTuple() {
		
		long now = System.currentTimeMillis();
		if (beginBatch == 0)
			beginBatch = now;
		if (tuplesSentInThisBatch < tupleRatePerBatch) {
			String msgIdString = msgIdPrefix + (msgId++);
			if (useAckers)
				collector.emit(new Values(TWEET), msgIdString);
			else
				collector.emit(new Values(TWEET));
			traceLantecy(msgIdString);
			if (throughputLog != null)
				throughputLog.println(System.currentTimeMillis() + ",1");
			tuplesSentInThisBatch++;
		} else {
			Thread.yield();
			if (now - beginBatch >= batchLength) {
				beginBatch += batchLength;
				tuplesSentInThisBatch = 0;
			}
		}
	}
	
	private void traceLantecy(String msgIdString) {
		if (logLatency) {
			latencyMap.put(msgIdString, System.nanoTime());
			tsMap.put(msgIdString, System.currentTimeMillis());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
	
	@Override
    public void ack(Object msgId) {
		if (logLatency) {
	        latencyLog.println(tsMap.get(msgId) + "," + Math.abs(System.nanoTime() - latencyMap.get(msgId)));
	        removeMsgId(msgId);
		}
    }

    @Override
    public void fail(Object msgId) {
    	removeMsgId(msgId);
    }
    
    private void removeMsgId(Object msgId) {
    	latencyMap.remove(msgId);
        tsMap.remove(msgId);
    }

	/*@Override
    public void close() {
		if (br != null)
			try {
				br.close();
			} catch (IOException e) {
				logger.error("Error closing file " + filename, e);
			}
    }*/
}
