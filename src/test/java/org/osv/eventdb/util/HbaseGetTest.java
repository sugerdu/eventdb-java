package org.osv.eventdb.util;

//import static org.junit.Assert.*;
//import static org.hamcrest.Matchers.*;
import java.util.*;
import org.junit.Test;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;
import java.util.*;

public class HbaseGetTest {
	@Test
	public void testHbaseGet() throws IOException {
		Get get = new Get(Bytes.toBytes("testRowKey"));

		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col3"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col4"));

		get.addColumn(Bytes.toBytes("other"), Bytes.toBytes("col1"));
		get.addColumn(Bytes.toBytes("other"), Bytes.toBytes("col2"));
		get.addColumn(Bytes.toBytes("other"), Bytes.toBytes("col3"));
		get.addColumn(Bytes.toBytes("other"), Bytes.toBytes("col4"));

		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		for (Map.Entry<byte[], NavigableSet<byte[]>> entry : map.entrySet()) {
			String rowkey = Bytes.toString(entry.getKey());
			System.out.println("rowkey: " + rowkey);
			for (byte[] col : entry.getValue())
				System.out.printf("%s ", Bytes.toString(col));
			System.out.printf("\n");
		}
	}
}