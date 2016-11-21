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
		conf.set( "hbase.zookeeper.quorum", "master");
	}
	
	public static void addData(String rowKey,String TableName,String[] col1,String[] val1,String[] col2,String[] val2) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		HTable table = new HTable(conf,TableName);
		put.addColumn(Bytes.toBytes("Data"), Bytes.toBytes("ID"), Bytes.toBytes("THUC_001"));
		put.addColumn(Bytes.toBytes("Data"), Bytes.toBytes("I"), Bytes.toBytes("0"));
		//THIS
		
		table.close();
	}
	
	
}
