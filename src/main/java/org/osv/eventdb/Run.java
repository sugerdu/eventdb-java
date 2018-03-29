package org.osv.eventdb;

import org.osv.eventdb.fits.io.*;

public class Run {
	public static void main(String[] args) throws Exception {
		if (args[0].equals("insertHeFits")) {
			HeFits2Hbase he = new HeFits2Hbase(args[1], args[2]);
			he.insert();
		}
	}
}
