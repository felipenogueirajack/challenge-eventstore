package net.intelie.challenges;
/**
 * Class that encodes and decodes long values using delta-enconding. 
 * 
 * @author Felipe Nogueira
 *
 */
public class DeltaEncodeDecode {

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
