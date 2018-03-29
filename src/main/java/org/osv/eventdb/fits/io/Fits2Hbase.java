package org.osv.eventdb.fits.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.davidmoten.hilbert.*;

import org.osv.eventdb.fits.evt.*;

import java.io.*;
import java.util.*;

public abstract class Fits2Hbase<E extends Evt>{
	protected int evtLength;
	private Table htable;
	private File fits;
	private SmallHilbertCurve hcurve;
	private Configuration hconf;
	private boolean fix = false;
	public Fits2Hbase(String pathname, String tablename, int len) throws IOException{
		hconf = HBaseConfiguration.create();
		hconf.set("hbase.zookeeper.property.clientPort", "2181");
		hconf.set("hbase.zookeeper.quorum", "sbd03,sbd05.ihep.ac.cn,sbd07.ihep.ac.cn");
		init(pathname, tablename, len);
	}
	public Fits2Hbase(String pathname, String tablename, Configuration hconf, int len) throws IOException{
		this.hconf = hconf;
		init(pathname, tablename, len);
	}
	public void setFix(boolean fix){
		this.fix = fix;
	}
	public void insert() throws IOException{
		if(fits.isFile()){
			insertFitsFile(fits.getAbsolutePath());
		}else if(fits.isDirectory()){
			for(File fl: fits.listFiles())
				insertFitsFile(fl.getAbsolutePath());
		}
	}
	private void init(String pathname, String tablename, int len) throws IOException{
		evtLength = len;
		Connection connection = ConnectionFactory.createConnection(hconf);
		htable = connection.getTable(TableName.valueOf(tablename));
		fits = new File(pathname);
		hcurve = HilbertCurve.small().bits(6).dimensions(3);
	}
	protected abstract FitsFile<E> getFitsFile(String filename);
	protected abstract E getEvt(byte[] evtBin) throws IOException;
	private void insertFitsFile(String filename) throws IOException{
		int preBucket = 0;
		HashMap<String, LinkedList<E>> map = new HashMap<String, LinkedList<E>>(262144, 0.9f);
		FitsFile<E> ff = getFitsFile(filename);
		for(E he: ff){
			double time = he.getTime();
			int timeCell = (int)Math.floor(time / 10.0);
			int timeBucket = timeCell / 64;
			int timeAxis = timeCell % 64;
			int eventType = (int)(he.getEventType() & 0x00ff);
			int detID = (int)(he.getDetID() & 0x00ff);
			int channel = (int)(he.getChannel() & 0x00ff) / 4;
			int pulse = (int)(he.getPulseWidth() & 0x00ff) / 4;
			long index = hcurve.index(timeAxis, channel, pulse);
			String rowkey = String.format("%d#%d#%07d#%06d", eventType, detID, timeBucket, index);
			if(preBucket == timeBucket){
				LinkedList<E> val = map.get(rowkey);
				if(val == null){
					val = new LinkedList<E>();
					val.add(he);
					map.put(rowkey, val);
				}else
					val.add(he);
			}else{
				if(preBucket != 0){
					//put
					System.out.printf("%s start to insert timeBucket:%d\n", (new Date()).toString(), timeBucket);
					if(fix)
						fixBucket(map);
					put(map);
					System.out.printf("%s finished to insert timeBucket:%d\n", (new Date()).toString(), timeBucket);
					map.clear();
				}
				preBucket = timeBucket;
				LinkedList<E> val = new LinkedList<E>();
				val.add(he);
				map.put(rowkey, val);
			}
		}
		//put remain
		if(fix)
			fixBucket(map);
		put(map);
		System.out.printf("Completed to insert fits file: %s", filename);
	}
	private void fixBucket(HashMap<String, LinkedList<E>> map) throws IOException{
		List<String> keys = new ArrayList<String>(map.keySet());
		List<Get> gets= new LinkedList<Get>();
		for(String key: keys){
			Get get = new Get(Bytes.toBytes(key));
			get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("events"));
			gets.add(get);
		}
		Result[] res = htable.get(gets);
		byte[] evtBin = new byte[evtLength];
		for(int i = 0; i < keys.size(); i++){
			LinkedList<E> events = map.get(keys.get(i));
			HashSet<E> eventsSet = new HashSet<E>(events);
			byte[] eventsBin = res[i].getValue(Bytes.toBytes("data"), Bytes.toBytes("events"));
			if(eventsBin != null){
				for(int j = 0; j < eventsBin.length; j += evtLength)
					for(int k = 0; k < evtLength; k++){
						evtBin[k] = eventsBin[j + k];
						eventsSet.add(getEvt(evtBin));
					}
			}
			events.clear();
			events.addAll(eventsSet);
		}
	}
	private void put(HashMap<String, LinkedList<E>> map) throws IOException{
		List<Row> batch = new LinkedList<Row>();
		int capacity = 100000;
		int num = 0;
		for(Map.Entry<String, LinkedList<E>> ent: map.entrySet()){
			String key = ent.getKey();
			LinkedList<E> val = ent.getValue();
			int size = val.size();
			byte[] coldata = new byte[evtLength * size];
			for(int j = 0; j < size; j++){
				byte[] eb = val.get(j).getBin();
				int idx = j * evtLength;
				for(int k = 0; k < evtLength; k++)
					coldata[idx + k] = eb[k];
			}
			Put put = new Put(Bytes.toBytes(key));
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("events"), coldata);
			put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("size"), Bytes.toBytes(String.valueOf(size)));
			batch.add(put);
			num++;
			if(num == capacity){
				Object[] results = new Object[batch.size()];
				try{
					htable.batch(batch, results);
				}catch(Exception e){
					System.out.println("Error: " + e);
				}finally{
					num = 0;
					batch.clear();
					System.out.println("finish batch");
				}
			}
		}
		Object[] results = new Object[batch.size()];
		try{
			htable.batch(batch, results);
		}catch(Exception e){
			System.out.println("Error: " + e);
		}
	}
}
