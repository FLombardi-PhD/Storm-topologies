package storm.starter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

public class DatasetSorter {

	public static void main(String[] args) throws Exception {
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
			TreeMap<Long, Collection<String>> map = new TreeMap<Long, Collection<String>>();
			String line;
			while ( (line = br.readLine()) != null ) {
				long k = Long.parseLong(line.substring(0, line.indexOf(',')));
				Collection<String> items = map.get(k);
				if (items == null) {
					items = new ArrayList<String>();
					map.put(k, items);
				}
				items.add(line);
			}
			br.close();
			
			PrintStream ps = new PrintStream(args[1]);
			for (Collection<String> items : map.values())
				for (String item : items)
					ps.println(item);
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
