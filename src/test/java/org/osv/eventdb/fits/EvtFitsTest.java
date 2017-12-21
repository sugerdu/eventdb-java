package org.osv.eventdb.fits;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import org.junit.Test;
import java.net.URI;
import nom.tam.fits.Fits;
import nom.tam.fits.BinaryTableHDU;
import nom.tam.util.*;
import java.io.FileInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.ByteArrayInputStream;
import org.apache.hadoop.io.DoubleWritable;
import org.davidmoten.hilbert.*;
import java.util.List;

public class EvtFitsTest{
	/*@Test
	public void testDemo() throws Exception{
		HeQuery.query(
				"HeFitsTest",
				174161400.066, 174162610.778,
				new int[]{12, 13},
				23, 64,
				1, 200,
				new int[]{0}
				);
	}
	*/
	/*
	@Test
	public void testHilbert() throws Exception{
		SmallHilbertCurve c = HilbertCurve.small().bits(8).dimensions(2);
		long[] point1 = new long[]{3, 3};
		long[] point2 = new long[]{14, 18};
		int splitDepth = 4;
		List<Range> ranges = c.query(point1, point2, splitDepth);
		for(Range rg: ranges)
			System.out.println(rg);
	}
	@Test
	public void testByteParse()
	throws Exception{
		FileInputStream fin = new FileInputStream("/root/eventdb/eventdb-java/data/R-ID0301-00000003-20170719-v1.FITS");
		fin.skip(2880 * 4);
		byte[] time = new byte[16];
		fin.read(time);
		ByteArrayInputStream timeIn = new ByteArrayInputStream(time);
		BufferedDataInputStream fs = new BufferedDataInputStream(timeIn);
		double dtime = fs.readDouble();
		System.out.printf("time: %f\n", dtime);
		DoubleWritable dwtime = new DoubleWritable(dtime);
		double getTime = dwtime.get();
		System.out.printf("DoubleWritable: %s\n", String.format("String.format: %.0f\n", getTime));
		//Det_ID
		System.out.println(String.format("%d", fs.readByte() & 0x00ff));
	}
	*/
}
