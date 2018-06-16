package at.illecker.sentistorm.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateHelper {

	/**
	 * convert date in this format '2014-04-26T11:54:39Z' to timestamp
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String date = "2014-04-26T11:54:39Z";
		System.out.println("Convert '" + date + "' to ts " + convert(date));

	}
	
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	public static long convert(String date) {
		try {
			// String tmp = date.replace('T', ' ').substring(0, date.length() - 1);
			Date d = dateFormat.parse(date);
			// System.out.println(d);
			return d.getTime();
		} catch(Throwable t) {
			t.printStackTrace();
			return -1;
		}
	}

}
