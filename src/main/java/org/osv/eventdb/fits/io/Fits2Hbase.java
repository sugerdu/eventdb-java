package org.osv.eventdb.fits.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.davidmoten.hilbert.*;
import org.osv.eventdb.fits.evt.*;
import org.osv.eventdb.util.*;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;

public abstract class Fits2Hbase<E extends Evt> implements Runnable {
	protected List<File> fits;
	protected int evtLength;
	protected SmallHilbertCurve hcurve;
	protected Configuration hconf;
	protected Connection hconn;
	protected Table htable;
	protected ConsistentHashRouter conHashRouter;
	protected Thread thread;

	private void init(List<File> fitsFiles, Configuration config, Connection conn, Table table, String tablename,
			int len, int regions) throws IOException {
		evtLength = len;
		fits = fitsFiles;

		if (config == null) {
			ConfigProperties configProp = new ConfigProperties();
			hconf = HBaseConfiguration.create();
			hconf.set("hbase.zookeeper.property.clientPort",
					configProp.getProperty("hbase.zookeeper.property.clientPort"));
			hconf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		} else {
			hconf = config;
		}

		if (conn == null)
			hconn = ConnectionFactory.createConnection(hconf);
		else
			hconn = conn;

		if (table == null)
			htable = hconn.getTable(TableName.valueOf(tablename));
		else
			htable = table;

		hcurve = HilbertCurve.small().bits(6).dimensions(3);

		if (regions > 1000 || regions < 1)
			throw new IOException("regions should be in 1~1000");
		List<PhysicalNode> regionPrefix = new ArrayList<PhysicalNode>();
		for (int i = 0; i < regions; i++)
			regionPrefix.add(new PhysicalNode(String.format("%03d", i)));
		conHashRouter = new ConsistentHashRouter(regionPrefix, 5);
	}

	public void close() throws IOException {
		hconn.close();
	}

	public Fits2Hbase(List<File> fitsFiles, String tablename, int len, int regions) throws IOException {
		init(fitsFiles, null, null, null, tablename, len, regions);
	}

	public Fits2Hbase(List<File> fitsFiles, Configuration config, String tablename, int len, int regions)
			throws IOException {
		init(fitsFiles, config, null, null, tablename, len, regions);
	}

	public Fits2Hbase(List<File> fitsFiles, Configuration config, Connection conn, Table table, int len, int regions)
			throws IOException {
		init(fitsFiles, config, conn, table, null, len, regions);
	}

	public void insert() throws IOException {
		insertFitsFile(fits);
	}

	public void run() {
		try {
			insertFitsFile(fits);
		} catch (Exception e) {
			System.out.println("Error: " + e);
		}
	}

	public void start() {
		if(thread == null)
			thread = new Thread(this);
		thread.start();
	}

	public Thread getThread(){
		return thread;
	}

	protected abstract FitsFile<E> getFitsFile(String filename);

	protected abstract E getEvt(byte[] evtBin) throws IOException;

	protected void insertFitsFile(File[] files) throws IOException {
		for (File file : files)
			insertFitsFile(file);
	}

	protected void insertFitsFile(List<File> files) throws IOException {
		for (File file : files)
			insertFitsFile(file);
	}

	protected void insertFitsFile(File file) throws IOException {
		if (file.isFile())
			insertFitsFile(file.getAbsolutePath());
		else
			throw new IOException(file.getAbsolutePath() + " is not a file");
	}

	protected void insertFitsFile(String filename) throws IOException {
		int preBucket = 0;
		int timeBucket = 0;
		SimpleDateFormat timeformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		HashMap<String, LinkedList<E>> map = new HashMap<String, LinkedList<E>>(262144, 0.9f);
		FitsFile<E> ff = getFitsFile(filename);
		for (E he : ff) {
			double time = he.getTime();
			int timeCell = (int) Math.floor(time / 10.0);
			timeBucket = timeCell / 64;
			int timeAxis = timeCell % 64;
			int eventType = (int) (he.getEventType() & 0x00ff);
			int detID = (int) (he.getDetID() & 0x00ff);
			int channel = (int) (he.getChannel() & 0x00ff) / 4;
			int pulse = (int) (he.getPulseWidth() & 0x00ff) / 4;
			long index = hcurve.index(timeAxis, channel, pulse);
			String prefix = conHashRouter.getNode(String.format("%d%d%d", timeBucket, eventType, detID)).getId();
			String rowkey = String.format("%s%d%d%d#%06d", prefix, timeBucket, eventType, detID, index);
			if (preBucket == timeBucket) {
				LinkedList<E> val = map.get(rowkey);
				if (val == null) {
					val = new LinkedList<E>();
					val.add(he);
					map.put(rowkey, val);
				} else
					val.add(he);
			} else {
				if (preBucket != 0) {
					//put
					put(map);
					System.out.printf("%s Finished to insert timeBucket(%s):%d\n", timeformat.format(new Date()),
							filename, timeBucket);
					map.clear();
				}
				preBucket = timeBucket;
				LinkedList<E> val = new LinkedList<E>();
				val.add(he);
				map.put(rowkey, val);
			}
		}
		//put remain
		put(map);
		System.out.printf("%s Completed to insert fits file: %s\n", timeformat.format(new Date()), filename);
		ff.close();
	}

	private void put(HashMap<String, LinkedList<E>> map) throws IOException {
		List<Row> batch = new LinkedList<Row>();
		int capacity = 20480;
		int num = 0;
		for (Map.Entry<String, LinkedList<E>> ent : map.entrySet()) {
			String key = ent.getKey();
			LinkedList<E> val = ent.getValue();
			int size = val.size();
			byte[] coldata = new byte[evtLength * size];
			for (int j = 0; j < size; j++) {
				byte[] eb = val.get(j).getBin();
				int idx = j * evtLength;
				for (int k = 0; k < evtLength; k++)
					coldata[idx + k] = eb[k];
			}
			Put put = new Put(Bytes.toBytes(key));
			put.setDurability(Durability.SKIP_WAL);
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("events"), coldata);
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("size"), Bytes.toBytes(String.valueOf(size)));
			batch.add(put);
			num++;
			if (num == capacity) {
				Object[] results = new Object[batch.size()];
				try {
					htable.batch(batch, results);
				} catch (Exception e) {
					System.out.println("Error: " + e);
				} finally {
					num = 0;
					batch.clear();
				}
			}
		}
		Object[] results = new Object[batch.size()];
		try {
			htable.batch(batch, results);
		} catch (Exception e) {
			System.out.println("Error: " + e);
		}
	}
}
