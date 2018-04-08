package org.osv.eventdb.fits.io;

import java.io.*;
import java.util.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.osv.eventdb.fits.evt.*;

public class HeFits2Hbase extends Fits2Hbase<HeEvt> {
	public HeFits2Hbase(List<File> fitsFiles, String tablename, int regions) throws IOException {
		super(fitsFiles, tablename, 16, regions);
	}

	public HeFits2Hbase(List<File> fitsFiles, Configuration hconf, String tablename, int regions) throws IOException {
		super(fitsFiles, hconf, tablename, 16, regions);
	}

	public HeFits2Hbase(List<File> fitsFiles, Configuration hconf, Connection hconn, Table htable, int regions)
			throws IOException {
		super(fitsFiles, hconf, hconn, htable, 16, regions);
	}

	@Override
	protected HeEvt getEvt(byte[] evtBin) throws IOException {
		return new HeEvt(evtBin);
	}

	@Override
	protected FitsFile<HeEvt> getFitsFile(String filename) {
		return new HeFitsFile(filename);
	}
}
