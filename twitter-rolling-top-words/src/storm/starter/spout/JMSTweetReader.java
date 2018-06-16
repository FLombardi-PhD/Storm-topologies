package storm.starter.spout;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class JMSTweetReader extends BaseRichSpout {
	
	private static final long serialVersionUID = 6218280345583579250L;
	
	private final String jmsServer;
	private int tweetReplicationFactor;
	private SpoutOutputCollector collector;
	private Connection connection; 
	private Session session;
	private MessageConsumer messageConsumer;
	private Logger logger;
	private String msgIdPrefix;
	private long msgId;
	private String spoutId;
	
	private boolean logThroughput;
	private PrintStream throughputLog;
	
	private boolean logLatency;
	private PrintStream latencyLog;
	private Map<String, Long> latencyMap;
	private Map<String, Long> tsMap;
	
	private boolean useAckers;
	
	public JMSTweetReader(String jmsServer, int tweetReplicationFactor, boolean useAckers, boolean logThroughput, boolean logLatency) {
		this.jmsServer = jmsServer;
		this.tweetReplicationFactor = tweetReplicationFactor;
		this.useAckers = useAckers;
		this.logThroughput = logThroughput;
		this.logLatency = logLatency;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(JMSTweetReader.class);
		
		spoutId = "[JMSTweetReader task " + context.getThisTaskId() + "] ";
		msgIdPrefix = "id-" + context.getThisTaskId() + "-" + System.currentTimeMillis() + "-";
		
		Properties props = new Properties();
		props.put("java.naming.factory.initial","org.jnp.interfaces.NamingContextFactory");
		props.put("java.naming.provider.url", jmsServer);
		props.put("java.naming.factory.url.pkgs","org.jboss.naming:org.jnp.interfaces");
		
		try {
			
			if (logThroughput)
				throughputLog = new PrintStream("throughput." + context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId() + ".csv");
			
			if (logLatency) {
				latencyLog = new PrintStream(context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId()  +".csv");
				latencyMap = new HashMap<String, Long>();
				tsMap = new HashMap<String, Long>();
			}
			
			InitialContext initialContext = new InitialContext(props);
			logger.info(spoutId + "Context created!");
			
			Queue queue = (Queue)initialContext.lookup("/queue/tweetQueue");
			logger.info(spoutId + "Queue looked up!");
			
			ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
			logger.info(spoutId + "Connection Factory looked up!");
			
			connection = cf.createConnection();
			logger.info(spoutId + "Connection created!");
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			logger.info(spoutId + "Session created!");
			
			messageConsumer = session.createConsumer(queue);
			logger.info(spoutId + "MessageConsumer created!");
			
			connection.start();
			logger.info(spoutId + "Connection started!");
			
		} catch (Exception e) {
			logger.error(spoutId + "Error initializing the connection to the JMS server", e);
		}
	}
	
	@Override
    public void activate() {
		logger.info(spoutId + "activated!!!");
    }

    @Override
    public void deactivate() {
    	try {
    		messageConsumer.close();
			session.close();
			connection.close();
	    	logger.info(spoutId + "deactivated!!!");
    	} catch (JMSException e) {
			logger.error(spoutId + "Error deactivating JMSTweetReader spout", e);
		}
    }
	
	@Override
	public void close() {
		logger.info(spoutId + "closed!!!");
	}

	@Override
	public void nextTuple() {
		// read ts, tweet from file
		String tweet = null;
		try {
			TextMessage messageReceived = (TextMessage)messageConsumer.receiveNoWait();
			// TextMessage messageReceived = (TextMessage)messageConsumer.receive();
			if (messageReceived != null) {
				tweet = messageReceived.getText();
				logger.debug(spoutId + "Received message: " + tweet);
			} else {
				Thread.sleep(1);
			}
		} catch (Exception e) {
			logger.error(spoutId + "Error in message reception", e);
		}
		
		// remove ts and send
		if (tweet != null) {
			tweet = tweet.substring(tweet.indexOf('\t') + 1).replace('\n', ' ');
			String msgIdString = null;
			for (int i = 0; i < tweetReplicationFactor; i++) {
				msgIdString = msgIdPrefix + (msgId++);
				if (useAckers)
					collector.emit(new Values(tweet), msgIdString);
				else
					collector.emit(new Values(tweet));
				traceLantecy(msgIdString);
			}
			if (throughputLog != null)
				throughputLog.println(System.currentTimeMillis() + "," + tweetReplicationFactor);
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
