package net.intelie.challenges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

public class IteratorTest {
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
	public void iterateTest() {
		ConcurrentEventStore store = new ConcurrentEventStore();
		String type = "typeA";
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 80l));
		
		EventIterator it =  store.query(type, 10l, 90l);
		
		assertTrue(it.moveNext());
		assertEquals(20, it.current().timestamp());

		assertTrue(it.moveNext());
		assertEquals(50, it.current().timestamp());
		
		assertTrue(it.moveNext());
		assertEquals(80, it.current().timestamp());
		
		assertFalse(it.moveNext());
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
	public void iteratorWithHistoryRemoveTest() {
		ConcurrentEventStore store = new ConcurrentEventStore(80);
		String type = "typeA";
		
		store.insert(new Event(type, 10l));
		store.insert(new Event(type, 20l));
		store.insert(new Event(type, 50l));
		store.insert(new Event(type, 80l));
		store.insert(new Event(type, 90l));
		
		store.encodeAndMoveToHistory(type);
		Collection<Event> events = store.getHistory().get(type).values();
		for (Event event : events) {
			System.out.println(event.timestamp());
		}

		EventIterator it =  store.query(type, 20, 81);
		
		
		assertTrue(it.moveNext()); //going to 20
		assertNotNull(it.current()); // 20
		
		assertTrue(it.moveNext()); //going to 50
		it.remove(); //removing 50
		
		assertTrue(it.moveNext()); //80
		assertNotNull(it.current()); 
		
		it =  store.query(type, 20, 81);
		Event current;
		while (it.moveNext()) {
			current = it.current();
			assertNotEquals(50, current.timestamp());
		}
	}
}
