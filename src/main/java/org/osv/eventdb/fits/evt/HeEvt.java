package org.osv.eventdb.fits.evt;

import java.io.IOException;
import nom.tam.util.*;
import java.io.ByteArrayInputStream;

public class HeEvt extends Evt{
	public HeEvt(){}
	public HeEvt(byte[] bin) throws IOException{
		super(bin);
	}
	@Override
	public void deserialize() throws IOException{
		BufferedDataInputStream evtParser = new BufferedDataInputStream(
				new ByteArrayInputStream(getBin()));
		setTime(evtParser.readDouble());
		setDetID(evtParser.readByte());
		setChannel(evtParser.readByte());
		setPulseWidth(evtParser.readByte());
		for(int i = 0; i < 3; i++)
			evtParser.readByte();
		setEventType(evtParser.readByte());
	}
}
