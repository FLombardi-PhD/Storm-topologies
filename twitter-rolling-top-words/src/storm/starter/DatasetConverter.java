package storm.starter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.codehaus.jettison.json.JSONObject;

import storm.starter.util.DateHelper;

public class DatasetConverter {

	public static void main(String[] args) throws Exception {
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
			PrintStream ps = new PrintStream(args[1]);
			
			String line;
			
			while ( (line = br.readLine()) != null ) {
				if (line.startsWith(","))
					line = line.substring(1);
				JSONObject json = new JSONObject(line);
				String date = json.getString("publishingdate");
				date = date.substring(2, date.length()-2);
				ps.println("" + DateHelper.convert(date) + "," + line);
			}
			
			br.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
