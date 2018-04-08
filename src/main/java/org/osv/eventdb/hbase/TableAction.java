package org.osv.eventdb.hbase;

import java.io.IOException;
import org.osv.eventdb.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

public class TableAction {
	private Configuration hconf;

	public TableAction() throws IOException {
		hconf = HBaseConfiguration.create();
		ConfigProperties configProp = new ConfigProperties();
		hconf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		hconf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
	}

	public TableAction(Configuration config) {
		hconf = config;
	}

	public void createTable(String tableName, int maxVersions, int splitNum) throws IOException {
		if (splitNum > 1000 || splitNum < 1)
			throw new IOException("splitNum should be in 1~1000");
		byte[][] splitKeys = new byte[splitNum - 1][];
		for (int i = 0; i < splitNum -1; i++)
			splitKeys[i] = Bytes.toBytes(String.format("%03d|", i));

		Connection con = ConnectionFactory.createConnection(hconf);
		Admin admin = con.getAdmin();
		TableName tname = TableName.valueOf(tableName);
		if (admin.tableExists(tname)) {
			admin.disableTable(tname);
			admin.deleteTable(tname);
		}
		HTableDescriptor tdesc = new HTableDescriptor(tname);
		HColumnDescriptor cdesc = new HColumnDescriptor(Bytes.toBytes("data"));
		cdesc.setMaxVersions(maxVersions).setBlocksize(65536).setBlockCacheEnabled(true)
				.setBloomFilterType(BloomType.ROW).setCompressionType(Compression.Algorithm.SNAPPY)
				.setTimeToLive(259200).setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);

		tdesc.addFamily(cdesc);
		admin.createTable(tdesc, splitKeys);

		admin.close();
		con.close();
	}

	public void deleteTable(String tableName) throws IOException {
		Connection con = ConnectionFactory.createConnection(hconf);
		Admin admin = con.getAdmin();
		TableName tname = TableName.valueOf(tableName);
		if (admin.tableExists(tname)) {
			admin.disableTable(tname);
			admin.deleteTable(tname);
		}
		admin.close();
		con.close();
	}
}
