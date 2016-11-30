package power;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.*;

public class transmit {
	static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master");
	}

	public static void main(String[] args) throws Exception {
		try {
			String TableName = "power";
			String family = "data";
			transmit.addData(TableName, "row1", family, "V", "24");
			transmit.addData(TableName, "row1", family, "I", "0");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addData(String TableName, String rowKey, String family, String qualifier, String value)
			throws IOException {
		try {
			HTable table = new HTable(conf, TableName);
			Put put = new Put(Bytes.toBytes(rowKey)); // �]�mRowKey
			// HColumnDescriptor[] columnFamilies =
			// table.getTableDescriptor().getColumnFamilies();

			// for (int i = 0; i < columnFamilies.length; i++) {
			// String familyName = columnFamilies[i].getNameAsString();
			// if (familyName.equals("col1")) {
			// for (int j = 0; j < columnFamilies.length; j++) {
			// put.add(Bytes.toBytes(familyName), Bytes.toBytes(col1[j]),
			// Bytes.toBytes(val1[j]));
			//
			// }
			// }
			// }
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void getAllRecord(String TableName, String rowKey) {
		try {
			HTable table = new HTable(conf, TableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
