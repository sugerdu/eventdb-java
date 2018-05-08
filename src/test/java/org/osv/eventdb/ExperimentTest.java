package org.osv.eventdb;

import java.util.Iterator;

import org.junit.Test;
import org.osv.eventdb.fits.evt.Evt;
import org.osv.eventdb.fits.evt.HeEvt;
import org.osv.eventdb.fits.io.HeFitsFile;

public class ExperimentTest {
	@Test
	public void testExperiment() throws Exception {
		HeFitsFile file = new HeFitsFile(
				"/home/ashin/ws/EventDB/hxmt/P010129900101-20170827-01-01/HXMT_P010129900101_HE-Evt_FFFFFF_V1_1RP.FITS");
		file.setBufferSize(1024);
		Iterator<HeEvt> it = file.iterator();
		for (int i = 0; i < 10; i++) {
			Evt evt = it.next();
			System.out.printf("%f %d %d %d %d\n", evt.getTime(), evt.getDetID() & 0x00ff, evt.getPulseWidth() & 0x00ff,
					evt.getChannel() & 0x00ff, evt.getEventType() & 0x00ff);
		}
	}
}