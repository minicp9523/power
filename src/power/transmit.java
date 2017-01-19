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
			conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) throws Exception {
		try {
			String TableName = "power";
			String family = "data";
			transmit.addData(TableName, "row1", family, "V", "33");
			transmit.addData(TableName, "row1", family, "I", "5");
			transmit.addData(TableName, "row1", family, "P", "222");
			
			getAllRecord(TableName, "row1");
			System.out.println("Success!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static void addData(String TableName, String rowKey, String family, String qualifier, String value)
			throws IOException {
		try {
			HTable table = new HTable(conf, TableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// ���������
	public static void getAllRecord(String TableName, String rowKey) {
		try {
			HTable table = new HTable(conf, TableName);
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
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
