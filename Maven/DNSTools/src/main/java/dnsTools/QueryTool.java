package dnsTools;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Map;
import java.util.Comparator;
import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * The main query class. This class parses the command line arguments, 
 * connents to the hive server, performs the request query and outputs
 * the formatted results to the standard output.
 * 
 */

public class QueryTool {

	// The name of the external hive table.
	private final String hiveTableName = "hive_table";
	
	// The name of the hbase tabled created using bulk-load operation.
	private final String hbaseTableName = "table1";

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private OptionsParser optionsParser = null;
	private Connection connection = null;
	private Statement stmt = null;
	
	/**
	 * Command line parser class from args4j package
	 * used to parse the command line arguments.
	 * 
	 */
	
	private class OptionsParser {

		@Option(name = "--rrset", usage = "query DNS resrouce record data set.")
		private boolean rrset;

		@Option(name = "--rrset_type", usage = "resrouce record type.", metaVar = "<sting>")
		private String rrset_type = "";

		@Option(name = "--rdata", usage = "query DNS response data.")
		private boolean rdata;

		@Option(name = "--rdata_type", usage = "response data type.", metaVar = "<sting>")
		private String rdata_type = "";

		@Option(name = "--query", usage = "string to query.", metaVar = "<sting>")
		private String query = "";

	}

	/**
	 * Compare two linked-list of strings element-by-elemnt.
	 * 
	 * @param l1, l2
	 *            The linked-lists to be compared.
	 *            
	 * @return The value 0 if the two linked-lists have the same length and are
	 *          equal element-by-element, and non-zero otherwise.
	 */
	
	private static int compareLists(LinkedList<String> l1, LinkedList<String> l2) {
		
		if (l1 == null && l2 == null)
			return 0;
		if (l1 == null) 
			return -1;
		if (l2 == null)
			return 1;
		
		int sizeDiff = l1.size() - l2.size();
		if (sizeDiff != 0) {
			return sizeDiff;
		}
		Iterator<String> it1 = l1.iterator();
		Iterator<String> it2 = l2.iterator();
		while (it1.hasNext()) {
			String item1 = it1.next();
			String item2 = it2.next();
			if (!item1.equals(item2)) {
				return item1.compareTo(item2);
			}
		}
		return 0;
	}
	
	/**
	 * Sort a the input map<key, value> based on its values.
	 * 
	 * @param map
	 *            The map that is to be sorted. 
	 *            
	 * @return A linked-list of <key, value> pairs found in the input
	 *         map sorted by values.
	 */
	
	private static LinkedList sortMapByValue(
			HashMap<String, LinkedList<String>> map) {

		// Sort the String linked-list that is mapped to each key.
		for (String key : map.keySet()) {
			LinkedList<String> strList = map.get(key);
			Collections.sort(strList);
		}

		// List of <key, value> pairs found in map.
		LinkedList list = new LinkedList(map.entrySet());
		
		// Sort the list using the given comparator which sorts on values.
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				LinkedList<String> strList1 = ((Map.Entry<String, LinkedList<String>>) o1)
						.getValue();
				LinkedList<String> strList2 = ((Map.Entry<String, LinkedList<String>>) o2)
						.getValue();
				
				return compareLists(strList1, strList2);
			}
		});

		return list;
	}

	/**
	 * Add input <key, value> pairs to a hashmap to obtain a 
	 * map of the form <key, linked-list values>.
	 * 
	 * @param key, value
	 *            The input <key, value> pair. 
	 *            
	 * @param map
	 *            The map where the resulting <key, linked-list values>
	 *            pairs are stored.  
	 */
	
	private static void addToMap(HashMap<String, LinkedList<String>> map,
			String key, String val) {
		LinkedList<String> l = new LinkedList<String>();
		if (map.get(key) == null) {
			l.add(val);
			map.put(key, l);
		} else {
			map.get(key).add(val);
		}
	}

	/**
	 * Process a query on the resource record data set. 
	 */

	private void processRRsetQuery() throws SQLException {
		String rrname = optionsParser.query;
		String rrtype = optionsParser.rrset_type;

		String sql = "select * from "
				+ hiveTableName
				+ String.format(" where rrtype = '%s' and rrname = '%s'",
						rrtype, rrname + ".");
		System.out.println("Running query: " + sql);
		ResultSet queryRes = stmt.executeQuery(sql);

		HashMap<String, LinkedList<String>> resultHash = new HashMap<String, LinkedList<String>>();
		while (queryRes.next()) {
			String resTimeStamp = queryRes.getString(2);
			String resRRName = queryRes.getString(3);
			String resRRType = queryRes.getString(4);
			String resRData = queryRes.getString(5);

			System.out.println(resTimeStamp + "\t" + resRRName + "\t" + resRRType + "\t" + resRData);

			// Group the results by timestamp field.
			addToMap(resultHash, resTimeStamp, resRData);
		}

		LinkedList<Map.Entry<String, LinkedList<String>>> sortedList = sortMapByValue(resultHash);

		System.out.println("\n\nQuery results:\n");
		// Lump the records in the sorted list that only differ in the timestamp field.
		LinkedList<String> prevList = null;
		LinkedList<String> currList = null;
		String currKey = null;
		String prevKey = null;
		int count = 0;
		String firstSeen = "";
		String lastSeen = "";
		for (Map.Entry<String, LinkedList<String>> me : sortedList) {
			currList = me.getValue();
			currKey = me.getKey();
			
			if (prevList == null) {
				prevKey = currKey;
				prevList = currList;
				continue;
			}
			
			if (firstSeen.compareTo(prevKey) > 0 || firstSeen.equals("")) {
				firstSeen = prevKey;
			}
			if (lastSeen.compareTo(prevKey) < 0) {
				lastSeen = prevKey;
			}
			count++;

			if (compareLists(prevList, currList) != 0) {
				System.out.println(String.format("{\"rrname\":\"%s\" ,\"rrtype\":\"%s\","
						+ " \"first_seen\":%s, \"count\":%d , \"last_seen\":%s, \"rdata\":\"%s\"}",
							rrname, rrtype, firstSeen, count, lastSeen, prevList));
					count = 0;
					firstSeen = "";
					lastSeen = "";
			}
			
			prevKey = currKey;
			prevList = currList;
		}

		if (firstSeen.compareTo(prevKey) > 0 || firstSeen.equals("")) {
			firstSeen = prevKey;
		}
		if (lastSeen.compareTo(prevKey) < 0) {
			lastSeen = prevKey;
		}
		count++;
		System.out.println(String.format("{\"rrname\":\"%s\" ,\"rrtype\":\"%s\","
				+ " \"first_seen\":%s, \"count\":%d, \"last_seen\":%s, \"rdata\":\"%s\"}",
				rrname, rrtype, firstSeen, count, lastSeen, prevList));
	}

	/**
	 * Process a query on the response data. 
	 */

	private void processRdataQuery() throws SQLException {
		String rdata = optionsParser.query;
		String rrtype = optionsParser.rdata_type;
		
		String rrtypeCondition = String.format(" where rdata = '%s'", rdata); 
		if (rrtype.equals("ip")) {
			rrtype = "A";
		} else if (rrtype.equals("dn")) {
			rrtype = "NS";
			rrtypeCondition = String.format(" where rdata = '%s'", rdata + ".");
		}
		
		String sql = "select * from " + hiveTableName
				+ rrtypeCondition;

		System.out.println("Running query: " + sql);
		ResultSet queryRes = stmt.executeQuery(sql);
		HashMap<String, LinkedList<String>> resultHash = new HashMap<String, LinkedList<String>>();
		while (queryRes.next()) {
			String resTimeStamp = queryRes.getString(2);
			String resRRName = queryRes.getString(3);
			String resRRType = queryRes.getString(4);
			String resRData = queryRes.getString(5);

			System.out.println(resTimeStamp + "\t" + resRRName
					+ "\t" + resRRType + "\t" + resRData);
			
			// Group the results by rrname field.
			addToMap(resultHash, resRRName, resTimeStamp);
		}

		System.out.println("\n\nQuery results:\n");
		for (String rrname : resultHash.keySet()) {

			LinkedList<String> list = resultHash.get(rrname);
			Collections.sort(list);

			System.out.println(String.format(
					"{\"rdata\":\"%s\", \"rrname\":\"%s\" ,\"rrtype\":\"%s\", \"first_seen\":%s,"
							+ " \"count\":%d, \"last_seen\":%s}", rdata,
					rrname, rrtype, list.peekFirst(), list.size(),
					list.peekLast()));
		}

	}

	/**
	 * Process the input query. First parse the command line arguments.
	 * If proper argument are given, connect to the hiverserver create a 
	 * hive external table based on the hbase data. Then perform the query
	 * give by the input arguements. 
	 */
	
	private void processQuery(String[] args) {

		optionsParser = new QueryTool().new OptionsParser();
		CmdLineParser parser = new CmdLineParser(optionsParser);

		parser.setUsageWidth(120);

		try {
			// Parse the command line arguments.
			parser.parseArgument(args);

			if (optionsParser.rrset && optionsParser.rdata)
				throw new CmdLineException(parser,
						"--rrset and --rdata flags cannot both be set.\n");

			if (optionsParser.query.equals(""))
				throw new CmdLineException(parser,
						"No query string was given.\n");

		} catch (CmdLineException e) {
			// Print the command usage if there is a problem 
			// in the command line arguments.
			System.err.println(e.getMessage());
			System.err.println("java hbase-pdns [options...]");
			parser.printUsage(System.err);
			System.err.println();

			// Print an example use of the command.
			System.err
					.println("  Example: java hbase-pdns --rrset --rrset_type A --query google.com");
			return;
		}

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		try {
			// Connect to hiveserver2 server.
			connection = DriverManager.getConnection(
					"jdbc:hive2://localhost:10000/default", "amir", "");

			stmt = connection.createStatement();

			// Create an external table in hive containing the hbase table 
			// created using bulk-load operation.
			stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "+ hiveTableName + "(key int, ts string, rrname string, rrtype string, rdata string) "
					+ "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
					+ " WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'cf1:ts,cf2:rrname,cf2:rrtype,cf3:rdata') "
					+ "TBLPROPERTIES(\"hbase.table.name\" = \"" + hbaseTableName + "\")");

			if (optionsParser.rdata) {
				processRdataQuery();
			} else if (optionsParser.rrset) {
				processRRsetQuery();
			}

		} catch (SQLException e) {
			System.err.println("Message: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		
		QueryTool qt = new QueryTool();
		qt.processQuery(args);
	}
}