package org.osv.eventdb.fits.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.davidmoten.hilbert.*;

import org.osv.eventdb.fits.evt.*;

import java.io.*;
import java.util.*;

public class HeFits2Hbase extends Fits2Hbase{
	private int skip2row;
	private int skip2begin;
	private int evtLength;
	public HeFits2Hbase(String pathname, String tablename) throws IOException{
		super(pathname, tablename);
		skip2row = 2880 * 2 + 80 * 4 + 9;
		skip2begin = 2880 * 4 - 2880 * 2 - 80 * 4 - 9 - 22;
		evtLength = 16;
	}
	@Override
	protected void insertFitsFile(String filename, Table table, SmallHilbertCurve curve) throws IOException{
		FileInputStream fin = new FileInputStream(filename);
		//get row number
		fin.skip(skip2row);
		byte[] brows = new byte[22];
		fin.read(brows);
		String rows = new String(brows);
		long rowCount = Long.valueOf(rows.trim());
		//seek at the beginning of event array
		fin.skip(skip2begin);

		byte[] evtBin = new byte[evtLength];
		int timeRange = 0;
		HashMap<String, ArrayList<HeEvt>> map = new HashMap<String, ArrayList<HeEvt>>();
		for(int i = 0; i < rowCount; i++){
			fin.read(evtBin);
			HeEvt he = new HeEvt(evtBin);
			int time = (int)Math.floor(he.getTime() / 10.0);
			int eventType = (int)(he.getEventType() & 0x00ff);
			int detID = (int)(he.getDetID() & 0x00ff);
			int channel = (int)(he.getChannel() & 0x00ff) / 4;
			int pulse = (int)(he.getPulseWidth() & 0x00ff) / 4;
			long index = curve.index(channel, pulse);
			String rowkey = String.format("%d#%d#%09d#%04d", eventType, detID, time, index);
			if(time == timeRange){
				ArrayList<HeEvt> val = map.get(rowkey);
				if(val == null){
					val = new ArrayList<HeEvt>();
					val.add(he);
					map.put(rowkey, val);
				}else
					val.add(he);
			}else{
				if(timeRange != 0){
					//put
					System.out.printf("%s start to insert timeRange:%d\n", (new Date()).toString(), timeRange);
					put(map, table);
					System.out.printf("%s end to insert timeRange:%d\n", (new Date()).toString(), timeRange);
					map.clear();
				}
				timeRange = time;
				ArrayList<HeEvt> val = new ArrayList<HeEvt>();
				val.add(he);
				map.put(rowkey, val);
			}
		}
		//put remain
		put(map, table);
		System.out.printf("Completed to insert fits file: %s", filename);
	}
	protected void put(HashMap<String, ArrayList<HeEvt>> map, Table table) throws IOException{
		List<Row> batch = new ArrayList<Row>();
		for(Map.Entry<String, ArrayList<HeEvt>> ent: map.entrySet()){
			String key = ent.getKey();
			ArrayList<HeEvt> val = ent.getValue();
			byte[] hkey = Bytes.toBytes(key);
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
		}
		Object[] results = new Object[batch.size()];
		try{
			table.batch(batch, results);
		}catch(Exception e){
			System.out.println("Error: " + e);
		}
	}
}
