package storm.starter.util;

import org.codehaus.jettison.json.JSONObject;

public class DummyHelper {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String line = "1397454814000,{\"cliptype\":[\"clip\"],\"body\":[\"#Renzi é convinto che anche la minoranza nel #pd lo seguirà..\nAnche perché se no n'do va?\nuna #poltrona ė almeno fino al 2018\n@FedeleAmico\"],\"publishingdate\":[\"2014-04-14T07:53:34Z\"],\"tweet_status_id\":[\"455735625099251712\"],\"locale\":[\"GMT\"],\"link\":[\"http://twitter.com/BoyFrank1/statuses/455735625099251712\"],\"author_screen_name\":[\"BoyFrank1\"],\"tweet_author_id\":[\"1207846141\"],\"tweet_type\":[\"tweet\"],\"author\":[\"frank boy\"],\"title\":[\"#Renzi é convinto che anche la minoranza nel #pd lo seguirà..\nAnche perché se no n'do va?\nuna #poltrona ė almeno fino al 2018\n@FedeleAmico\"],\"rank\":[\"97\"],\"tweet_mention\":[\"FedeleAmico\"],\"sourcecategory\":[\"UGC\"],\"migrated_flow_uri\":[\"its-455735625099251712\"],\"feed\":[\"its\"],\"author_id\":[\"1207846141\"],\"tweet_author_followers\":[518],\"sourcetype\":[\"SOCIAL NETWORK\"],\"rankClass\":[\"High/09/97\"],\"entityid\":[\"602069331\",\"1207846141\"],\"tweet_author_friends\":[579],\"tweet_author_screen_name\":[\"BoyFrank1\"],\"provider\":[\"intext\"],\"tweet_hashtag\":[\"pd\",\"Renzi\",\"poltrona\"],\"uri\":[\"wd-af4e736265e577fbf6d01f3ab4e3e7db6f6512f2\"],\"sourceuri\":[\"http://twitter.com/BoyFrank1/\"],\"sourcedomain\":[\"http://twitter.com/\"],\"language\":[\"it\"],\"rankClassH1\":[\"High\"],\"sourcelabel\":[\"frank boy\"]}";
		// long k = Long.parseLong(line.substring(0, line.indexOf(',')));
		// System.out.println(k);
		// line = line.replace('\n', ' ');
		line = line.substring(line.indexOf(',') + 1).replace('\n', ' ');
		System.out.println("tweet: " + line);
		
		JSONObject json = new JSONObject(line);
		String text = json.getString("body");
		text = text.substring(2, text.length() - 2);
		System.out.println("body: " + text);
		
		// String words[] = text.split("[ \"!�$%&/()='?^�@�#�,;.:-_]+");
		String words[] = text.split("[ .,]+");
		for (String s : words)
			System.out.println(s);
	}

}
