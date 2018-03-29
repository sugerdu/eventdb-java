package org.osv.eventdb.fits.io;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.osv.eventdb.fits.evt.*;

public class HeFits2Hbase extends Fits2Hbase<HeEvt>{
	public HeFits2Hbase(String pathname, String tablename) throws IOException{
		super(pathname, tablename, 16);
	}
	public HeFits2Hbase(String pathname, String tablename, Configuration hconf) throws IOException{
		super(pathname, tablename, hconf, 16);
	}
	@Override
	protected HeEvt getEvt(byte[] evtBin) throws IOException{
		return new HeEvt(evtBin);
	}
	@Override
	protected FitsFile<HeEvt> getFitsFile(String filename){
		return new HeFitsFile(filename);
	}
}
