package net.intelie.challenges;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
		assertEquals(event.isCompressed(), false);

    }
	
	@Test
	private void eventIsCompressedTest() {
		
		Event event = new Event("some_type", 123L, true);
        assertEquals(event.isCompressed(), true);
        
        event = new Event("some_type", 123L, false);
        assertEquals(event.isCompressed(), false);
	}

}