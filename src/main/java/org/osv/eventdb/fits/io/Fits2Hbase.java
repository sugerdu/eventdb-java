package org.osv.eventdb.fits.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.davidmoten.hilbert.*;

import org.osv.eventdb.fits.evt.*;

import java.io.*;
import java.util.*;

public abstract class Fits2Hbase{
	private Table htable;
	private File fits;
	private SmallHilbertCurve hcurve;
	public Fits2Hbase(String pathname, String tablename) throws IOException{
		Configuration hconf = HBaseConfiguration.create();
		hconf.set("hbase.zookeeper.property.clientPort", "2181");
		hconf.set("hbase.zookeeper.quorum", "sbd03,sbd05.ihep.ac.cn,sbd07.ihep.ac.cn");
		Connection connection = ConnectionFactory.createConnection(hconf);
		htable = connection.getTable(TableName.valueOf(tablename));
		fits = new File(pathname);
		hcurve = HilbertCurve.small().bits(6).dimensions(2);
	}
	public void insert() throws IOException{
		if(fits.isFile()){
			insertFitsFile(fits.getAbsolutePath(), htable, hcurve);
		}else if(fits.isDirectory()){
			for(File fl: fits.listFiles())
				insertFitsFile(fl.getAbsolutePath(), htable, hcurve);
		}
	}
	protected abstract void insertFitsFile(String filename, Table talbe, SmallHilbertCurve curve) throws IOException;
}
