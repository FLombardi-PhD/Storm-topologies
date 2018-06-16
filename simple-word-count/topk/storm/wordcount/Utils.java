package storm.wordcount;

import java.util.Map;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public class Utils {

	private Utils() {}
	
	public static boolean isTickTuple(Tuple tuple) {
		return
				tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
	public static String[] getTopK(Map<String, Long> counterMap, int K) {
		String topK[] = new String[K];
		
		for (String word : counterMap.keySet()) {
			
			for (int i = 0; i < K; i++) {
				
				if (topK[i] == null || counterMap.get(word) > counterMap.get(topK[i])) {
					if (topK[i] != null)
						for (int j = K - 1; j > i; j--)
							topK[j] = topK[j - 1];
					topK[i] = word;
					break;
				}
				
			} /* end topK array iteration */
			
		} /* end counterMap enumeration */
		
		return topK;
	}
}
