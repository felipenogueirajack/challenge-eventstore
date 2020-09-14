package net.intelie.challenges;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DeltaEncodeDecodeTest {

	@Test
	public void encodeTest() {
		long delta = DeltaEncoderDecoder.encode(10000, 10000);
		assertEquals(0, delta);
		
		delta = DeltaEncoderDecoder.encode(10005, 10000);
		assertEquals(5, delta);
	}
	
	@Test
	public void decodeTest() {
		long time = DeltaEncoderDecoder.decode(10000, 10000);
		assertEquals(20000, time);
		
		time = DeltaEncoderDecoder.decode(0, 10000);
		assertEquals(10000, time);
		
		time = DeltaEncoderDecoder.decode(5, 10000);
		assertEquals(10005, time);
	}
}
