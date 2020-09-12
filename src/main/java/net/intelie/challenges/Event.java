package net.intelie.challenges;

/**
 * This is just an event stub, feel free to expand it if needed.
 */
public class Event {
    private final String type;
    private final long timestamp;
	
	/** indicates if the event is historical, its timestamp is compressed **/
    private final boolean isCompressed;


    public Event(String type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public String type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }
	
	public boolean isCompressed() {
    	return isCompressed;
    }

}
