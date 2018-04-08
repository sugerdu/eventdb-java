package org.osv.eventdb;

import java.io.*;
import java.util.*;
import org.osv.eventdb.fits.io.*;
import org.osv.eventdb.hbase.*;

public class Run {
	public static void main(String[] args) throws Exception {
		if (args[0].equals("insertHeFits")) {
			//pathname, talbename, regions, [threads]
			String pathname = args[1];
			String tablename = args[2];
			int regions = Integer.valueOf(args[3]);
			File fits = new File(pathname);
			if (fits.isFile()) {
				List<File> files = new ArrayList<File>();
				files.add(fits);
				HeFits2Hbase he = new HeFits2Hbase(files, tablename, regions);
				he.insert();
				he.close();
			} else {
				File[] files = fits.listFiles();
				int threads = 20;
				if (args.length == 5)
					threads = Integer.valueOf(args[4]);
				if (threads > files.length)
					threads = files.length;
				int fileNum = files.length / threads;
				int fileIndex = 0;
				System.out.printf("%s fits files are executed in %s threads\n", files.length, threads);
				for (int i = 0; i < threads; i++) {
					List<File> fitsList = new ArrayList<File>();
					for (int j = 0; j < fileNum; j++)
						fitsList.add(files[fileIndex++]);
					if (i == threads - 1)
						while (fileIndex < files.length)
							fitsList.add(files[fileIndex++]);
					HeFits2Hbase he = new HeFits2Hbase(fitsList, tablename, regions);
					he.start();
				}
			}

		} else if (args[0].equals("createTable")) {
			TableAction taction = new TableAction();
			//tablename maxVersions regions
			taction.createTable(args[1], Integer.valueOf(args[2]), Integer.valueOf(args[3]));
			System.out.printf("success to create table(%s)\n", args[1]);

		} else if (args[0].equals("deleteTable")) {
			TableAction taction = new TableAction();
			//talbename
			taction.deleteTable(args[1]);
			System.out.printf("success to delete table(%s)\n", args[1]);
		}
	}
}
