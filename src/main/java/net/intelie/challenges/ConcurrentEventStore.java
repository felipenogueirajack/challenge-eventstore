package net.intelie.challenges;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A concurrent implementation of the EventSource interface.
 * 
 * <p>
 * The events, according to their type, are stored in a separate
 * {@link ConcurrentSkipListMap} using the timestamp as the key. As such, two
 * distinct Event objects with the same type and timestamp are not allowed.
 * The events SkipListMap are stored in {@link ConcurrentHashMap} using the
 * event's type as the key. This provides access to each skipListMap with
 * constant time.
 * 
 * <p>
 * As {@link ConcurrentSkipListMap} implements a SkipList, it provides log(n)
 * time cost for the many operations (such as get, put and remove). 
 * In the worst case, the skip list has O(nlogn) space complexity, as it has 
 * log n layers and n elements implemented as sorted linked list in the lowest 
 * layer. For the {@code query} operation, this implementation uses 
 * the subMap method of the SkipListMap, which returns a view of of the
 * portion of the original map whose keys lies within the given range.
 * 
 * <p>
 * Insertion,  removal, update, and access operations safely execute
 * concurrently by multiple threads. While allows the concurrent
 * iteration with other operations, the iterator is 
 * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>. It does 
 * <em>not</em> throw {@link java.util.ConcurrentModificationException
 * ConcurrentModificationException}. 
 * 
 * Another feature of this implementation is that historical events are kept a part
 * in a different map. The method {@code encodeAndMoveToHistory} can be called 
 * by a job in order to move the events of the given type from the main map to
 * the historical one. In addition, also for space concerns, the timestamps 
 * of the historical series are compressed. For simplicity, it was assumed that
 * "historical" data does not receives events to be inserted. 
 * The compression algorithm implemented was also very simple, 
 * as the objective was to show that historical events can be stored 
 * a part and be compressed for better performance.
 * 
 * <p>
 * Also for simplicity, it is assumed that events with the same type and
 * timestamp are the same event. If this is not the case and the EventStore 
 * should handle timestamp repetitions, the implementation must be adapted.
 * One way to do just that is to change the skip list, mapping the the timestamp
 * to an thread-safe list of events. In this case, the list will store more
 * than one element only when there is a timestamp repetition. 
 * In fact, if there is a lot of event data, indexing by their timestamp alone 
 * will affect performance. It is possible to have group of events 
 * indexed by a function of the timestamp, for instance those that happened in 
 * the same minute. A point of attention is that, now, the list would 
 * have different timestamps and the query method result must be sorted. 
 * If the focus is performance of the query result, is to better to keep the 
 * list sorted, as a binary search that runs in log n time cost can be used.
 * 
 * 
 * <p>
 * In the case of arrays, the array 
  
  
 * @author Felipe Nogueira
 *
 */
public class ConcurrentEventStore implements EventStore {

		
	/** Map of events indexed by their type 
	 * 
	 * if there is an estimate for the number of concurrent threads, we might
	 * set the constructor parameters (initial size, load factor, 
	 * and concurrency level) accordingly for better performance. 
	 * **/
	
	private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Event>> 
		eventMap = new ConcurrentHashMap<>();
	
	/** Map of historical events indexed by their type **/
	private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Event>> 
		historyMap = new ConcurrentHashMap<>();
	
	/** Map the store the original non-enconded timestamps for each type
	 *  only used when dealing with historical events **/
	private final ConcurrentHashMap<String, Long> historicalTimestamps =
			new ConcurrentHashMap<String, Long>();

	/** timestamp limit of history data. an event can only be moved to the history 
	 * if its timestamp is lower than this value.  **/ 
	private final long historyTimestampLimit ;
	
	
	public ConcurrentEventStore() {
		this.historyTimestampLimit = 10;
	}
	
	public ConcurrentEventStore(long limit) {
		this.historyTimestampLimit = limit;
	}
	
	/**
	 * Returns the map of events.  
	 * Created only to help the execution of unit tests.
	 * 
	 * @return the map of events indexed by events type
	 */
	
	public Map<String, ConcurrentSkipListMap<Long, Event>> getEvents() {
		return eventMap;
	}
	
	/**
	 * Returns the map of history events.  
	 * Created only to help the execution of unit tests.
	 * 
	 * @return the map of events indexed by events type
	 */
	
	public Map<String, ConcurrentSkipListMap<Long, Event>> getHistory() {
		return historyMap;
	}

	/**
	 * Insert the event in average log(n) time cost in the EventSource.
	 * 
	 * @param The event to inserted
	 * @throws NullPointerException if the specified event is null
	 */

	@Override
	public void insert(Event event) {
		checkNotNull(event);

		// If there is no skipListMap for the event type, it has to be created.
		// The event is inserted in the corresponding skipListMap.
		// This is done within the compute operation, which is atomically
		// executed.
		
		eventMap.compute(event.type(), (key, value) -> {
			if (value == null) {
				value = new ConcurrentSkipListMap<>();
			}
			value.put(event.timestamp(), event);
			return value;
		});
	}
	
	/** Removes all events of a given type from the EventSource 
	 *  in constant time cost. 
	 *  
	 *  @param type   The type of events to be removed.
	 *  @throws NullPointerException if the given type is null or empty;
	 */

	public void removeAll(String type) {
		checkNotNull(type);
		eventMap.remove(type);
		historyMap.remove(type);
	}

	/**
	 * Returns an iterator for the events of a given type and whose timestamps
	 * range from {@code startTime}, inclusive, to {@code endTime}, 
	 * exclusive.
	 * It runs in O(log n), as we can access the skipList of the given time in a
	 * constant time and the search for startTime and endTime runs in about 
	 * in O(log n) each.
	 * 
	 * It looks for the events in the main map and also in the history map. 
	 * The timestamps in the history maps have been delta-encoded to save
	 * memory space. Clients of this API have to decode the timestamp of historic 
	 * events. A boolean attribute of the Event class indicates
	 * if the timestamp is encoded or not. To decoded it, the current timestamp
	 * must be added to the first of this historic, which can be retrieved by 
	 * the method {@code getFirstHistoricalTimestamp}. 
	 * 
	 * @param type      The type we are querying for.
	 * @param startTime Start timestamp (inclusive).
	 * @param endTime   End timestamp (exclusive).
	 * @return An iterator where all its events have same type as {@code type} and
	 *         timestamp between {@code startTime} (inclusive) and {@code endTime}
	 *         (exclusive). 
	 * @throws IllegalArgumentException if {@code type} is null or if
	 * {@code startTime} is zero  or if {@code startTime} is greater or 
	 * equal to {@code endTime}, or if there is no events with {@code type} 
	 * queried for.
	 */

	@Override
	public EventIterator query(String type, long startTime, long endTime) {

		if (type == null || startTime == 0 || startTime >= endTime) {
			throw new IllegalArgumentException("invalid query arguments");
		}
		
		//events can always be in the main event map 
		//regardless of their timestamp
		ConcurrentSkipListMap<Long, Event> events = eventMap.get(type);
		ConcurrentSkipListMap<Long, Event> history = historyMap.get(type);
		
		if (history == null && events == null) {
			throw new IllegalArgumentException("no events of given type");
		}
		
		if (startTime < historyTimestampLimit && history != null) {
			
			//searching in history
			ConcurrentNavigableMap<Long, Event> subHistory = historySubMap(type, startTime, endTime);
			if (events == null) {
				//all events queried for are in history
				return new ConcurrentEventStoreIterator(subHistory.values().iterator());
			}
			
			//events in both history and main series
			ConcurrentNavigableMap<Long, Event> subMap = events.subMap(startTime, endTime);
			return new ConcurrentEventStoreIterator(subHistory.values().iterator(),
					subMap.values().iterator());
		}
		
		//only in the main map 
		ConcurrentNavigableMap<Long, Event> subMap = events.subMap(startTime, endTime);
		return new ConcurrentEventStoreIterator(subMap.values().iterator());
	}
	
	/**
	 * Returns the part of the historical events of the given type
	 * whose keys range from  fromKey, inclusive, to toKey, exclusive. 
	 * (If fromKey and toKey are equal, the returned map is empty.) 
	 * 
	 * As the historical series were created using compressed timestamps and
	 * indexes, the search in the history must use compressed startTime and
	 * compressed endTime
	 * 
	 * @param type the type of events
	 * @param Start timestamp (inclusive).
	 * @param endTime   End timestamp (exclusive).
	 * @return the submap containing the historical series
	 */
	
	private ConcurrentNavigableMap<Long, Event> historySubMap(String type, long startTime, long endTime ) {
		long historicEndTime = Math.min(endTime, historyTimestampLimit);
		
		long first = getFirstHistoricalTimestamp(type);
		
		long compressedStart = encode(startTime, first);
		long compressedEnd = encode(historicEndTime, first);
		
		ConcurrentSkipListMap<Long, Event> history = historyMap.get(type);
		return history.subMap(compressedStart, compressedEnd);
		
	}
	
	/**
	 * 
	 * <p>Compress the timestamps of the events of the given type using 
	 * delta-encoding. Only the first timestamp of the given type is kept and, for the
	 * remaining ones, only the difference between them and the first timestamp 
	 * are stored. It runs in O(n) time complexity, as n being the number
	 * of events of the given type.
	 * 
	 * <p>If those historic events are queried, the decompression should take 
	 * place for the non-compressed timestamps to be displayed correctly. 
	 * A boolean attribute the Event class indicates if its timestamp is 
	 * compressed or not. 
	 * 
	 * <p>This method can be called by a job that 
	 * runs periodically checking which events might be compressed and migrated 
	 * to the history map. In addition, not to have more concurrent operations
	 * in the eventMap, a single-thread execution is encouraged. 
	 * 
	 * <p>As the Event class has a final timestamp attribute, it is not possible 
	 * to update and just move the object from one map to the history map; 
	 * a new event object needs to be created with the offset/delta as the timestamp 
	 * (as mentioned, the first element keeps his original timestamp). We
	 * kept the original timestamps as indexes in the historyMap. 
	 * 
	 * <p>It is public only for testing.
	 * 
	 * As an example, if the timestamps begins with 111110, 111112, 111115, the 
	 * result compressed series would be: 111110, 2, 5. 
	 * For simplicity, the first timestamp (111110) is stored in a secondary 
	 * map, so all historic series can be saved using compressed timestamps 
	 * and indexes.  
	 * 
	 *
	 * @param type type of the events to be moved to history
	 */
	
	public void encodeAndMoveToHistory(String type) {
		checkNotNull(type);
		ConcurrentSkipListMap<Long, Event> events = eventMap.get(type);
		if (events == null) {
			return;
		}
		Long firstKey = events.firstKey();
		Long lastKey = Math.min(events.lastKey(), historyTimestampLimit);
		ConcurrentNavigableMap<Long, Event>  subMap = events.subMap(firstKey, lastKey);

		//if there is already this type in history, there is already a first non-encoded timestamp
		Long firstTimestamp = getFirstHistoricalTimestamp(type);
		Iterator<Event> iterator = subMap.values().iterator();
		
		while(iterator.hasNext()) {
			Event event = iterator.next();
			Event compressedEvent;
			long originalTime =  event.timestamp();
			
			if (firstTimestamp == null) {
				firstTimestamp = event.timestamp();
				historicalTimestamps.put(type, firstTimestamp);
			} 
			compressedEvent = createCompressedEvent(event, firstTimestamp);
			insertInHistory(compressedEvent, originalTime);
			iterator.remove();
		}
	}
	
	public Long getFirstHistoricalTimestamp(String type) {
		checkNotNull(type);
		Long originaltime = historicalTimestamps.get(type);
	
		return (originaltime == null) ? null : originaltime;
	}
	
	public long encode(long time, long firstTimestamp) {
		return time - firstTimestamp;
	}
	
	/** Compress the timestamp of the event. It stores only the difference
	 * between the timestamp the given event with the given {@code firstTimestamp}
	 * Illustrates that historical series might be compressed in order to save
	 * memory.
	 * 
	 * @param event
	 * @param firstTimestamp
	 * @return
	 */
	public Event createCompressedEvent(Event event, long firstTimestamp) {
		
		//the timestamp attribute of the Event class is final 
		//we need to create another event and remove the original one 
		//from the map. if otherwise we could just move the object
		
		long delta = event.timestamp() - firstTimestamp;
		Event compressedEvent = new Event(event.type(), delta, true); 
		
		return compressedEvent;
	}
	

	/**
	 * Insert events in the history map
	 * public for testing
	 * @param event event to be inserted
	 * @throws NullPointerException is given event is null
	 */
	
	public void insertInHistory (Event event, long originalTimestamp) {
		checkNotNull(event);
		historyMap.compute(event.type(), (key, value) -> {
			if (value == null) {
				value = new ConcurrentSkipListMap<>();
			}
			value.put(event.timestamp(), event);
			return value;
		});
	}

	/**
     * Throws NullPointerException if argument is null.
     *
     * @param object the argument
     */
	
	private static void checkNotNull(Object object) {
		if (object == null)
			throw new NullPointerException();
	}
	
}
