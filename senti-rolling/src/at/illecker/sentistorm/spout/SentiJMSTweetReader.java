package at.illecker.sentistorm.spout;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.jms.MessageConsumer;
import javax.jms.Queue;

import org.apache.log4j.Logger;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class SentiJMSTweetReader extends BaseRichSpout {

	private static final long serialVersionUID = 3028853846518561027L;

	public static final String ID = "tweet-reader";

	SpoutOutputCollector m_collector;

	/** variables for JMS **/
	private final String jmsServer;
	private int tweetReplicationFactor;
	private Connection connection;
	private Session session;
	private MessageConsumer messageConsumer;


	/** variables for logger, msg and task id **/
	private static final Logger logger = Logger.getLogger(SentiJMSTweetReader.class);
	private String msgIdPrefix;
	private String spoutId;
	private int taskId;
	private long timeout;


	/** variables for logging latency, fail, throughput **/
	private boolean logThroughput;
	private PrintStream throughputLog;
	private boolean logLatency;
	private PrintStream latencyLog;
	private Map<Long, Long> latencyMap;
	private Map<Long, Long> tsMap;
	private Map<Long, Long> tuplesFailedMap;
	private boolean logFail;
	private PrintStream failLog;
	private PrintStream tuplesFailedLog;
	private boolean useAckers;


	/** variabless added to obtain the context **/
	private static final String SPOUT_NAME = "tweet-reader";
	private static final String PARALL_HINT_SPOUT_DESC = "parallelism_hint:";
	private static final String TOPOLOGY_TASK_SPOUT_DESC = "topology.tasks\":";

	private long m_messageId = 0;

	private int tuple_current_second = 0;
	private long current_second;
	private int second_update = 1;
	private int rate = 100;
	private int ratio_stream = 2;
	
	/** constructor method **/
	public SentiJMSTweetReader(String jmsServer, int tweetReplicationFactor, boolean useAckers, boolean logThroughput, boolean logLatency, boolean logFail, int timeout) {
		this.jmsServer = jmsServer;
		this.tweetReplicationFactor = tweetReplicationFactor;
		this.useAckers = useAckers;
		this.logThroughput = logThroughput;
		this.logLatency = logLatency;
		this.logFail = logFail;
		this.timeout = (long)timeout * (long)Math.pow(10, 9);
	}

	/** constructor method **/
	public SentiJMSTweetReader(String jmsServer, int tweetReplicationFactor, boolean useAckers, boolean logThroughput, boolean logLatency, boolean logFail, int timeout, int second_update, int rate, int ratio_stream) {
		this.jmsServer = jmsServer;
		this.tweetReplicationFactor = tweetReplicationFactor;
		this.useAckers = useAckers;
		this.logThroughput = logThroughput;
		this.logLatency = logLatency;
		this.logFail = logFail;
		this.timeout = (long)timeout * (long)Math.pow(10, 9);
		this.second_update = second_update;
		this.rate = rate;
		this.ratio_stream = ratio_stream;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// key of output tuples
		//declarer.declare(new Fields("id", "score", "text"));
		declarer.declareStream("senti-stream", new Fields("id", "score", "text"));
		declarer.declareStream("rolling-stream", new Fields("tweet"));
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
				
		this.m_collector= collector;
		logger.info("Collector loaded correctly.");

		taskId = context.getThisTaskId();
		spoutId = "[SentiJMSTweetReader task " + taskId + "] ";
		//msgIdPrefix = "id-" + context.getThisTaskId() + "-" + System.currentTimeMillis() + "-";
		msgIdPrefix = taskId + "";
		logger.info("TaskID read correctly from context: " + taskId);
		
		// dirty: read the parallelism level from context
		String spoutSpec = context.getRawTopology().get_spouts().get(SPOUT_NAME).toString();
		int startIndex = spoutSpec.indexOf(PARALL_HINT_SPOUT_DESC) + PARALL_HINT_SPOUT_DESC.length();
		String parallHintSubString = spoutSpec.substring(startIndex);
		@SuppressWarnings("unused")
		int endIndex = parallHintSubString.indexOf(',');
		
		// dirty: same way as above to read the task
		startIndex = parallHintSubString.indexOf(TOPOLOGY_TASK_SPOUT_DESC) + TOPOLOGY_TASK_SPOUT_DESC.length();
		String taskSubString = parallHintSubString.substring(startIndex);
		endIndex = taskSubString.indexOf('}');
		logger.info("Parallelism of spout read correctly from context.");
		
		Properties props = new Properties();
		props.put("java.naming.factory.initial","org.jnp.interfaces.NamingContextFactory");
		props.put("java.naming.provider.url", jmsServer);
		props.put("java.naming.factory.url.pkgs","org.jboss.naming:org.jnp.interfaces");
		logger.info("Properties put correctly.");

		try {

			if (logThroughput)
				throughputLog = new PrintStream("throughput." + context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId() + ".csv");

			if (logLatency) {
				latencyLog = new PrintStream(context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId()  +".csv");
				latencyMap = new HashMap<Long, Long>();
				tsMap = new HashMap<Long, Long>();
			}

			if (logFail) {
				failLog = new PrintStream("fail."+context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId()  +".csv");
				tuplesFailedLog = new PrintStream("tuples_failed." + context.getStormId() + "." + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis() + "." + context.getThisTaskId()  +".csv");
			}

			tuplesFailedMap = new HashMap<Long, Long>();

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
		String tweet = null;
		try {
			TextMessage messageReceived = (TextMessage)messageConsumer.receiveNoWait();
			//TextMessage messageReceived = (TextMessage)messageConsumer.receive();
			if (messageReceived != null) {
				tweet = messageReceived.getText();
				tweet = tweet.substring(tweet.indexOf('\t') + 1).replace('\n', ' ');
				logger.debug(spoutId + "Received message: " + tweet);
				
				// added conversion to json and body extraction				
				JSONObject json = null;
				try {
					// try to parse json; if some quotations there exist, replace them and try to parse again
					try {
						json = new JSONObject(tweet);
					} catch (JSONException e) {
						logger.warn(spoutId + "Reparsing tweet " + tweet);
						String inputNoQuotations = tweet.replaceAll("\"", "'");
						String inputBodyQuotations = inputNoQuotations
								.replaceFirst("'body':", "\"body\":")
								.replace("['","[\"").replace("']", "\"]");
						json = new JSONObject(inputBodyQuotations);
					}
					String body = json.getString("body");
					body = body.substring(2, body.length() - 2); // remove [" and "]

					// now replicate the message and send them
					String msgIdString = null;
					long msgIdNum = 0;
					for (int i = 0; i < tweetReplicationFactor; i++) {

						msgIdString = msgIdPrefix + (m_messageId++);
						msgIdNum = Long.valueOf(msgIdString).longValue();
						Random random = new Random();
						int minScore = -5;
						int maxScore = 5;
						int randomScore = random.nextInt(maxScore-minScore) + minScore;

						if (useAckers) {
							// send even tuples to rolling-top topology
							if (msgIdNum % ratio_stream == 0) {
								
								// solution to send a fixed rate
								long timestamp = System.currentTimeMillis();
								if (timestamp > ( current_second + (second_update * 1000)) ) {
									current_second = timestamp;
									tuple_current_second = 1;
									m_collector.emit("rolling-stream", new Values(tweet), msgIdNum);
									traceLantecy(msgIdNum);
									tuplesFailedMap.put(msgIdNum, msgIdNum);
								}
								else {
									if (tuple_current_second < rate) {
										tuple_current_second++;
										m_collector.emit("rolling-stream", new Values(tweet), msgIdNum);										
										traceLantecy(msgIdNum);
										tuplesFailedMap.put(msgIdNum, msgIdNum);
									}
									else {
										// drop tuple to take fixed the input rate
									}
								}
							}
							
							// send odd tuples to senti-storm topology
							else {
								m_collector.emit("senti-stream", new Values(
										body.hashCode() + (Math.random()*1000), // use hashcode + random to simulate original Tweet.getId()
										randomScore, // use random score to simulate original Tweet.getScore()
										body), // finally, tweet simply contain the text to be processed
										msgIdNum);
								traceLantecy(msgIdNum);
								tuplesFailedMap.put(msgIdNum, msgIdNum);
							}
						}								
						else {
							// send even tuples to rolling-top topology
							if (msgIdNum % 2 == 0) {
								m_collector.emit("rolling-stream", new Values(tweet));
							}
							// send odd tuples to senti-storm topology
							else {
								m_collector.emit("senti-stream", new Values(
										body.hashCode() + (Math.random()*1000), // use hashcode + random to simulate original Tweet.getId()
										randomScore, // use random score to simulate original Tweet.getScore()
										body)); // finally, tweet simply contain the text to be processed
							}
							traceLantecy(msgIdNum);
							tuplesFailedMap.put(msgIdNum, msgIdNum);
						}	

						
					}

					if (throughputLog != null)
						throughputLog.println(System.currentTimeMillis() + "," + tweetReplicationFactor);

				} catch (JSONException e) {
					logger.error(spoutId + "Error parsing tweet " + tweet, e);
				}				
			} else {
				Thread.sleep(1);
			}
		} catch (Exception e) {
			logger.error(spoutId + "Error in message reception", e);
		}		
	}

	@Override
	public void ack(Object msgId) {
		if (logLatency) {
			latencyLog.println(tsMap.get(msgId) + "," + Math.abs(System.nanoTime() - latencyMap.get(msgId)));
			removeMsgId(msgId);
			tuplesFailedMap.remove(msgId);
		}
	}

	@Override
	public void fail(Object msgId) {
		if (logFail) {
			long failLatency = Math.abs(System.nanoTime() - latencyMap.get(msgId));
			if (failLatency >= timeout) {
				failLog.println(tsMap.get(msgId) + "," + failLatency);    			
				tuplesFailedLog.println(tuplesFailedMap.get(msgId));
				/* for debugging false tuples failure */
				//Exception ex = new Exception("stacktrace for logging fail() invocation");
				logger.error("failing msgId " + msgId + " with latency " + failLatency + " (timeout " + timeout + ")");  		
				removeMsgId(msgId);
				tuplesFailedMap.remove(msgId);
			}
			else
				ack(msgId);
		}
	}
	
	private void traceLantecy(long msgIdString) {
		if (logLatency) {
			latencyMap.put(msgIdString, System.nanoTime());
			tsMap.put(msgIdString, System.currentTimeMillis());
		}
	}

	private void removeMsgId(Object msgId) {
		latencyMap.remove(msgId);
		tsMap.remove(msgId);
	}
	
}
