package net.intelie.challenges;

import java.util.Iterator;
import java.util.Base64.Decoder;

public class ConcurrentEventStoreIterator implements EventIterator {
	
	private Iterator<Event> historyIterator;
	private Iterator<Event> mainIterator;
	
	private Event current;
	private boolean isHistorical;
	private long timestamp;
	
	public ConcurrentEventStoreIterator(Iterator<Event> iterator) {
		if (iterator ==  null) {
			throw new NullPointerException();
		}
		this.mainIterator = iterator;
	}
	
	/**
	 * It creates an iterator of events that can iterate in the 
	 * historical event series and also in the main one. 
	 * 
	 * @param historyIt iterator of the history collection
	 * @param mainIt iterator of the main collection
	 * @throws NullPointerException if both given iterators are null
	 */
	
	public ConcurrentEventStoreIterator(Iterator<Event> historyIt, Iterator<Event> mainIt, long timestamp) {
		if (historyIt == null && mainIt == null) {
			throw new NullPointerException();
		}
		this.historyIterator = historyIt;
		this.mainIterator = mainIt;
		this.timestamp = timestamp;
	}
		
	/**
	 * Move the iterator to the next event, if there is one. 
	 * 
	 * @return {@code true} if the iterator was moved to the next event, 
	 * {@code false} otherwise
	 * @throws IllegalStateException if {@link #moveNext} was never called
     *                               or its last result was {@code false}.
	 */
	
	@Override
	public boolean moveNext() {
		
		if (historyIterator != null && historyIterator.hasNext()) {
			current = historyIterator.next();
			isHistorical = true;
			return true;
		}
		
		if (mainIterator != null && mainIterator.hasNext()) {
			current = mainIterator.next();
			isHistorical = false;
			return true;
		}
		
		return false;
		
	}

	@Override
	public void close() throws Exception {
		historyIterator = null;
		mainIterator = null;
		current = null;
		
	}

	/**
	 * Returns the current event of the iteration. 
	 * If the event is historical, its timestamp is compressed, so 
	 * it decompresses, creating a new event with its original noncompressed
	 * timestamp. In that way, only when this method is called, the decompression
	 * takes place.   
	 * 
	 * @return the current event
	 * @throws IllegalStateException if {@link #moveNext} was never called
     *                               or its last result was {@code false}.
	 */

	@Override
	public Event current() {
		if (current == null) {
			throw new IllegalStateException();
		}
		
		if (isHistorical) {
			//decompressing the timestamp of historical series
			long original = DeltaEncoderDecoder.decode(current.timestamp(), timestamp);
			
			return new Event(current.type(), original);
		}
		
		return current;
	}

	/**
	 * Removes the current event of the iteration
	 */
	
	@Override
	public void remove() {
		
		if (current == null) {
			throw new IllegalStateException();
		}
		
		if (isHistorical) {
			historyIterator.remove();
		} 
		
		else {
			mainIterator.remove();
		}
	}
	

	
	

}
