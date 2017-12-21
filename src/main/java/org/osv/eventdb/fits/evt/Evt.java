/****************************************************************************
*     FileName: Evt.java
*         Desc: Abstract class to deserialize fits event
*       Author: Ashin Gau
*        Email: ashingau@outlook.com
*     HomePage: http://www.ashin.space
*      Version: 0.0.1
*   LastChange: 2018-03-20 13:19:50
*      History:
****************************************************************************/
package org.osv.eventdb.fits.evt;

import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * Used to deserialize fits event.
 */
public abstract class Evt implements WritableComparable<Evt>{
	private byte[] bin;				// Serialized byte stream of fits event
	private int length;				// The length of bin
	private double time;
	private byte detID;
	private byte channel;
	private byte pulseWidth;
	private byte eventType;

	public Evt(){}
	/**
	 * Creates an object by deserializing byte stream.
	 * @param bin Serialized byte stream.
	 * @throws IOException If failed to deserialize.
	 */
	public Evt(byte[] bin) throws IOException{
		setBin(bin);
		deserialize();
	}

	/**
	 * The method to deserialize byte stream and implemented by subclass.
	 * @throws IOException If failed to deserialize;
	 */
	public abstract void deserialize() throws IOException;

	/**
	 * Get serialized byte stream.
	 */
	public byte[] getBin(){
		return bin;
	}
	/**
	 * Set serialized byte stream.
	 * @param bin Serialized byte stream.
	 */
	public void setBin(byte[] bin){
		length = bin.length;
		this.bin = new byte[length];
		for(int i = 0; i < length; i++)
			this.bin[i] = bin[i];
	}
	public double getTime(){
		return time;
	}
	public void setTime(double time){
		this.time = time;
	}
	public byte getDetID(){
		return detID;
	}
	public void setDetID(byte detID){
		this.detID = detID;
	}
	public byte getChannel(){
		return channel;
	}
	public void setChannel(byte channel){
		this.channel = channel;
	}
	public byte getPulseWidth(){
		return pulseWidth;
	}
	public void setPulseWidth(byte pulseWidth){
		this.pulseWidth = pulseWidth;
	}
	public byte getEventType(){
		return eventType;
	}
	public void setEventType(byte eventType){
		this.eventType = eventType;
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeInt(length);
		out.write(bin);
	}
	@Override
	public void readFields(DataInput in) throws IOException{
		length = in.readInt();
		bin = new byte[length];
		for(int i = 0; i < length; i++)
			bin[i] = in.readByte();
	}
	@Override
	public int compareTo(Evt evt){
		Double x = getTime();
		return x.compareTo(evt.getTime());
	}
}
