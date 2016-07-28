package MUDROD.Datamining.tools;

/*
 * HeapItem.java
 *
 * Note: this implementation comes from the Extension Data Structures course
 */

/**
 * HeapItem - a class that is used for items stored in a priority
 * queue (e.g., a heap).  Associated with each item is a priority, and
 * the class' compareTo() method compares items according to their
 * priorities.
 */
public class HeapItem {
    private Object data;
    private long priority;

    public HeapItem(Object data, long priority) {
	this.data = data;
	this.priority = priority;
    }

    public Object getData() {
	return data;
    }

    public long getPriority() {
	return priority;
    }

    public int compareTo(HeapItem other) {
	long diff = priority - other.priority;
	if (diff > 1e-6)
	    return 1;
	else if (diff < -1e-6)
	    return -1;
	else
	    return 0;
    }

    public String toString() {
	if (data == null) {
	    if ((int)priority == priority)
		return "" + (int)priority;
	    else
		return "" + priority;
	} else
	    return data + "(priority = " + priority + ")";
    }
}
