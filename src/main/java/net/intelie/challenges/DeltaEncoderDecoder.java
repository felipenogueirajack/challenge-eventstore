package net.intelie.challenges;
/**
 * Class that encodes long values using delta-encoding and 
 * decodes delta-encoded values. 
 * In Delta encoding, only the difference between two values are stored. 
 * In the following time series of 100000, 100000, 100001, 100005, the return
 * compressed would be 100000 (as the first is kept as reference), 0, 1, 5.
 * 
 * @author Felipe Nogueira
 *
 */
public class DeltaEncoderDecoder {

	/**
	 * Delta-encodes the given timestamp, using the firstTimestamp 
	 * as the reference. 
	 * 
	 * @param timestamp timestamp to be enconded
	 * @param firstTimestamp the reference timestamp
	 * @return the delta-encoded timestamp
	 */
	
	public static long encode(long timestamp, long firstTimestamp) {
		return timestamp - firstTimestamp;
	}
	
	
	/**
	 * It decodes the given timestamp, using the firstTimestamp 
	 * as the reference.
	 * 

	 * @param timestamp timestamp to be decoded
	 * @param firstTimestamp the reference timestamp
	 * @return the original (non-encoded) timestamp
	 */
	public static long decode(long timestamp, long firstTimestamp) {
		return timestamp + firstTimestamp;
				
	}

}
