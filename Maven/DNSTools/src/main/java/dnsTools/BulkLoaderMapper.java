package dnsTools;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.xbill.DNS.Message;
import org.xbill.DNS.utils.hexdump;
import org.apache.commons.codec.binary.Hex;

/**
 * Main mapper class. This class parses the input file and decodes the embedded
 * DNS packet. It Then looks for the ANSWER section in the packet to find the
 * DNS resource records. Then for each resouce record, it emits four key-value
 * pairs corresponding to the timestamp, rrname, rrtype, and rdata fields of the
 * resource record.
 * 
 */

public class BulkLoaderMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	// Mininum numer of fields in a DNS resouce record.
	private final static int MIN_RR_LENGTH = 4;

	// Number of fields in each line of text file.
	private final static int LINE_SEG_FIELDS = 4;

	/**
	 * HBase table columns and column families enumeration.
	 */
	private enum ColumnEnum {
		COL_RRNAME("rrname".getBytes(), "cf".getBytes(), 0), COL_RRTYPE("rrtype"
				.getBytes(), "cf".getBytes(), 3), COL_RDATA(
				"rdata".getBytes(), "cf".getBytes(), 4);

		private final byte[] columnName;
		private final byte[] columnFamily;
		private final int fieldNum;

		ColumnEnum(byte[] columnName, byte[] columnFamily, int fieldNum) {
			this.columnName = columnName;
			this.columnFamily = columnFamily;
			this.fieldNum = fieldNum;
		}

		private byte[] getColumnName() {
			return this.columnName;
		}

		private byte[] getColumnFamily() {
			return this.columnFamily;
		}

		private int getFieldNum() {
			return this.fieldNum;
		}
	}

	private byte[] intToByteArray(int i) {
		byte[] result = new byte[4];

		result[0] = (byte) (i >> 24);
		result[1] = (byte) (i >> 16);
		result[2] = (byte) (i >> 8);
		result[3] = (byte) i;

		return result;
	}

	private static Message parsePacket(String packet, Context context) {
		try {
			byte[] decodedPacket = Hex.decodeHex(packet.toCharArray());
			return new Message(decodedPacket);
		} catch (Exception e) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return null;
		}
	}

	private void hbase_write(LinkedList<String> rrecord_list, Context context) throws IOException, InterruptedException {
		
		if (rrecord_list.isEmpty())
			return;

		int rrname_field = ColumnEnum.COL_RRNAME.getFieldNum();
		int rrtype_field = ColumnEnum.COL_RRTYPE.getFieldNum();
		int rdata_field = ColumnEnum.COL_RDATA.getFieldNum();

		String rrtype = null;
		String rdata = null;
		String rrname = null;
		
		String[] rrFields = null;
		LinkedList<byte[]> rdata_list = new LinkedList<>();
		for (String rrecord: rrecord_list) {
			rrFields = rrecord.trim().split("[ \t]+");

			System.out.println("Line segments:" + rrFields.length
				+ "\n");

			if (rrFields.length < MIN_RR_LENGTH)
				return;

			if (rrname != null) {
				if (!rrFields[rrname_field].equals(rrname))
					return;
			}
			else
				rrname = rrFields[rrname_field];

			if (rrtype != null) {
				if (!rrFields[rrtype_field].equals(rrtype))
					return;
			}
			else
				rrtype = rrFields[rrtype_field];
			
			rdata_list.add(rrFields[rdata_field].getBytes());

			for (int i = 0; i < rrFields.length; i++) {
				System.out.println(rrFields[i] + "\n");
			}
		}
		
		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;
		byte[] column_name = null;
		byte[] column_family = null;
		
		// Set row-key using an increasing counter.
		//hKey.set(intToByteArray((int) context.getCounter(
		//		"HBaseKVMapper", "NUM_MSGS").getValue()));

		hKey.set(rrname.getBytes());

		column_name = ColumnEnum.COL_RRTYPE.getColumnName();
		column_family = ColumnEnum.COL_RRTYPE.getColumnFamily();
		if (!rrtype.equals("")) {
			kv = new KeyValue(hKey.get(), column_family, column_name,
					rrtype);
			context.write(hKey, kv);
		}
		
		int total_len = 0;
		for (byte[] rdata_: rdata_list) {
			total_len += rdata_.length;
		}
		byte[] rdata_byte_array = new byte[4 * (1 + rdata_list.size()) + total_len];
		int pos = 0;
		System.arraycopy(intToByteArray(rdata_list.size()), 0, rdata_byte_array, pos, 4);
		pos += 4;
		for (byte[] rdata_: rdata_list) {
			System.arraycopy(intToByteArray(rdata_.length), 0, rdata_byte_array, pos, 4);
			pos += 4;
			System.arraycopy(rdata_, 0, rdata_byte_array, pos, rdata_.length);
			pos += rdata_.length;
		}

		column_name = ColumnEnum.COL_RDATA.getColumnName();
		column_family = ColumnEnum.COL_RDATA.getColumnFamily();
		if (!rdata.equals("")) {
			kv = new KeyValue(hKey.get(), column_family, column_name,
					rdata_byte_array);
			context.write(hKey, kv);
		}

	}
	
	private enum ParseState {
		PARSE_START, ANSWER_START, ANSWER_END;
	}

	/**
	 * Parse a line of the input file and decode the embedded DNS packet. Emit
	 * four key-value pairs corresponding to the timestamp, rrname, rrtype, and
	 * rdata fields for each resouce record in the ANSWERS section of the
	 * decoded packet.
	 * 
	 * @param key
	 *            Long key corresponding to each line of input. This value is
	 *            not used in this function.
	 * 
	 * @param value
	 *            A single line of the input file to MR.
	 * 
	 * @param context
	 *            The context passed by the MR framework.
	 */

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] lineSegs = null;

		// Parse the input file by splitting each line assuming that
		// each line is an array of space or tab seperated fields.
		try {
			lineSegs = value.toString().split("[ \t]+");
		} catch (Exception ex) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		// Skip mal-formed lines.
		if (lineSegs.length != LINE_SEG_FIELDS)
			return;

		String timeStamp = lineSegs[0].substring(1);
		String logVersion = lineSegs[1];
		String srcIP = lineSegs[2];

		// Decode the DNS packet.
		Message packet = null;
		packet = parsePacket(lineSegs[3], context);
		ParseState state = ParseState.PARSE_START;
		LinkedList<String> rrecord_list = null;
		if (packet != null) {

			StringTokenizer st = new StringTokenizer(packet.toString(), "\n");
			while (st.hasMoreTokens()) {

				// The resouce record.
				String rrecord = st.nextToken();

				if (rrecord.trim().equals(""))
					continue;

				// Parse the resource record looking for the ANSWERS section.
				// For each resource record in this section emit the
				// corresponding
				// key-value pairs.
				switch (state) {
				case PARSE_START:
					if (rrecord.trim().startsWith(";; ANSWERS")) {
						state = ParseState.ANSWER_START;
						System.out.println("answer start\n");
						rrecord_list = new LinkedList<String>();
					}
					break;

				case ANSWER_START:
					if (rrecord.trim().startsWith(";;")) {
						state = ParseState.ANSWER_END;
						hbase_write(rrecord_list, context);
						System.out.println("answer end\n");
						break;
					}

					rrecord_list.add(rrecord);

					System.out.println("context counter: "
							+ context.getCounter("HBaseKVMapper", "NUM_MSGS")
									.getValue() + "\n");

					context.getCounter("HBaseKVMapper", "NUM_MSGS")
							.increment(1);

					break;

				case ANSWER_END:
				default:
					break;
				}

				if (state == ParseState.ANSWER_END)
					break;

			}

		}
	}
}