package org.osv.eventdb.util;

//import static org.junit.Assert.*;
//import static org.hamcrest.Matchers.*;
import java.util.*;
import org.junit.Test;

public class BitArrayTest {
	@Test
	public void testBitArrayOp() throws Exception {
		BitArray ba = new BitArray("10001000");
		ba.set(7).clear(4).and(new BitArray("10000000")).or(new BitArray("00000010")).not().nor(new BitArray("11000000"));

		System.out.println(ba.toString());
	}
}