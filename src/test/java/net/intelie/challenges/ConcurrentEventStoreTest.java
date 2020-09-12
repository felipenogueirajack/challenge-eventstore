package net.intelie.challenges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;


public class ConcurrentEventStoreTest extends EventStoreBaseTest{

	@Test
	public void insertTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		
		String type = "typeA";
		String anotherType = "typeB";
		
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(anotherType, 40l));
		
		Map<Long, Event> eventsTypeA = store.getEvents().get(type);
		Map<Long, Event> eventsTypeB = store.getEvents().get(anotherType);
		
		assertNotNull(eventsTypeA);
		assertNotNull(eventsTypeA.get(20l));
		assertNotNull(eventsTypeA.get(50l));
		assertNotNull(eventsTypeA.get(80l));
		
		assertNotNull(eventsTypeB);
		assertNotNull(eventsTypeB.get(40l));
	}
	
	@Test
	public void eventsSortingTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		
		//generating random timestamps to be inserted
		for (int i = 1; i <= 100 ; i++) {
			long random = (long) (Math.random() * 1000);
			store.insert(new Event("SortingTest", random));
		}
		
		Map<Long, Event> events = store.getEvents().get("SortingTest");
		assertNotNull(events);
		
		Iterator<Event> it = events.values().iterator();
		Event current = it.next();
		
		while (it.hasNext()) {
			Event next = it.next();
			assertFalse(current.timestamp() > next.timestamp());
			current = next;
		}
	}
	
	
	@Test
    public void testConcurrentInsert(){
    	ConcurrentEventStore store = new ConcurrentEventStore();
    	ExecutorService e = Executors.newFixedThreadPool(5);
    	for(int i =0; i < 5000; i++){
           e.submit(new Runnable(){
               public void run(){
            	   long random = (long) (Math.random() * 1000);
       			   store.insert(new Event("test", random));
               } 
           });
       }
    }
	
	public void removeAllTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		String type = "typeA";
		
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(type, 81l));
		store.insert(new Event(type, 10l));
		store.removeAll(type);
		assertNull(store.getEvents().get(type));
		assertNull(store.getHistory().get(type));
	}
	
	@Test
	public void iteratorIllegalStateTest() {
		
		ConcurrentEventStore store = new ConcurrentEventStore();
		String type = "typeA";
		
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(type, 81l));
		store.insert(new Event(type, 10l));

		EventIterator it =  store.query(type, 1l, 5l);
		try {
			it.current();
		} catch (IllegalStateException e) {
			assertNotNull(e);
		}
		
		try {
			it.remove();
		} catch (IllegalStateException e) {
			assertNotNull(e);
		}
	}
	
	@Test
	public void iteratorRemoveTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		String type = "typeA";
		
		store.insert(new Event(type, 10l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(type, 81l));
		
		EventIterator it =  store.query(type, 20l, 81l);
		
		assertTrue(it.moveNext()); //going to 20
		assertNotNull(it.current()); // 20
		
		assertTrue(it.moveNext()); //going to 50
		it.remove(); //removing 50
		
		assertTrue(it.moveNext()); //80
		assertNotNull(it.current()); 
		
		Collection<Event> events = store.getEvents().get(type).values();
		for (Event event : events) {
			assertTrue(event.timestamp() != 50l);
		}
	}
	
	@Test
	public void queryInvalidArgumentsTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		try{
			store.query(null, 1, 2);
		} catch (IllegalArgumentException e) {
			assertNotNull(e);
		}
		
		try{
			store.query("querytest", 0, 2);
		} catch (IllegalArgumentException e) {
			assertNotNull(e);
		}
		
		try{
			store.query("querytest", 2, 1);
		} catch (IllegalArgumentException e) {
			assertNotNull(e);
		}
	}
	
	
	@Test
	public void queryRangeTest() {
		
		ConcurrentEventStore store = new ConcurrentEventStore();
		String type = "typeA";
		
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(type, 81l));
		store.insert(new Event(type, 10l));
		
		EventIterator it =  store.query(type, 20l, 81l);
		Event current = null;
		while (it.moveNext()) {
			if (current == null) {
				current = it.current();
				assertEquals(20l, current.timestamp());
			}
			current = it.current();
			assertTrue(current.timestamp() >= 20l);
			assertTrue(current.timestamp() < 81l);
		}
		assertEquals(80l, current.timestamp());
	}

	/** Tests related to the history/compression **/
	@Test
	public void encondeTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		
		Event event = store.createCompressedEvent(new Event("type", 1111111115l), 1111111110l);
		assertEquals(5, event.timestamp());
	}
	
	@Test
	public void insertHistoryTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		store.insertInHistory(new Event("type", 5, true), 1111111110l);
		
		Event event = store.getHistory().get("type").firstEntry().getValue();
		assertNotNull(event);
	}
	
	@Test
	public void compressionTest() {
		
		ConcurrentEventStore store = new ConcurrentEventStore(20);
		String type = "compressionTest";
		
		//generating random timestamps to be inserted and storing in a set
		
		long first = 10;
		long increase = 1;
		for (long i = first ; i < 20; i=i+increase) {
			store.insert(new Event(type, i));
		}
		store.encodeAndMoveToHistory(type);
		
		Map<String, ConcurrentSkipListMap<Long, Event>> history = store.getHistory();
		ConcurrentSkipListMap<Long, Event> historyEvents = history.get(type); 
		assertNotNull(historyEvents);

		Collection<Event> compressed = historyEvents.values();
		int i = 0;
		for (Event event : compressed) {
			long compressedtime = i * increase;  
			assertEquals(compressedtime, event.timestamp());
			assertTrue(event.isCompressed());
			i++;
		}
	}
	
	@Test
	public void queryWithHistoryDataTest() {
		long historyLimit = 20;
		String type = "typeA";

		ConcurrentEventStore store = new ConcurrentEventStore(historyLimit);
		
		long first = 10;
		long increase = 1;
		for (long i = first ; i < 30; i=i+increase) {
			store.insert(new Event(type, i));
		}
		store.encodeAndMoveToHistory(type);
		
		//moving some to history...
		store.encodeAndMoveToHistory(type);
		
		long startTime = first;
		long endTime = 28;
		
		EventIterator it = store.query(type, startTime, endTime);
		Event current = null;
		int i = 0;
		while (it.moveNext()) {
			current = it.current();
			long originaltime = i + first;
			
			if (originaltime < historyLimit) {
				long compressedtime = i * increase;
				assertEquals(compressedtime, current.timestamp());
				assertTrue(current.isCompressed());
			}
			
			else {
				assertEquals(originaltime, current.timestamp());
				assertFalse(current.isCompressed());
			}
			i++;
		}
	}
	
	@Test
	public void iteratorWitHistoryRemoveTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		String type = "typeA";
		
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(type, 81l));
		store.insert(new Event(type, 10l));
		
		EventIterator it =  store.query(type, 20l, 81l);
		
		assertTrue(it.moveNext()); //going to 20
		assertNotNull(it.current()); // 20
		
		assertTrue(it.moveNext()); //going to 50
		it.remove(); //removing 50
		
		assertTrue(it.moveNext()); //80
		assertNotNull(it.current()); 
		
		Collection<Event> events = store.getEvents().get(type).values();
		for (Event event : events) {
			assertTrue(event.timestamp() != 50l);
		}
	}
	

}
