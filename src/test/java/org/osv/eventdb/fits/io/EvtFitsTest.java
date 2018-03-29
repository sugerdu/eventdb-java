package org.osv.eventdb.fits.io;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import org.junit.Test;
import org.osv.eventdb.fits.evt.*;

public class EvtFitsTest{
	@Test
	public void fitsIO(){
		HeFitsFile ff = new HeFitsFile("/root/eventdb/eventdb-java/data/R-ID0301-00000008-20170719-v1.FITS");
		int cnt = 0;
		for(HeEvt evt: ff){
			System.out.println(evt.getTime());
			cnt++;
			if(cnt > 10)
				break;
		}
	}
}
