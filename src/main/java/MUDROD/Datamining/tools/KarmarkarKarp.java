package MUDROD.Datamining.tools;

import java.util.*;

public class KarmarkarKarp {
    private static int DEFAULT_NUM = 100;

    public static void main(String[] args) {
        /*int numItems = DEFAULT_NUM;
        if (args.length > 1) {
            try {
                numItems = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("numItems must be an integer: " + args[1]);
                System.exit(2);
            }
        }
        long[] data = NumericDataReader.arrayFromFile(args[0], numItems);
        long residue = KK(data, numItems);*/
    	
    	long[] data = new long[10];
    	for(int i=0;i<10;i++){
    		data[i] = i;
    	}
    	long residue = KK(data, 10);
        System.out.println(residue);
    }

    public static long KK(long[] data, int numItems)
    {
        if (numItems > data.length)
            throw new IllegalArgumentException("numItems > data length");

        if (numItems == 0)
            return 0;

        HeapItem[] hdata = new HeapItem[numItems];
        for (int i = 0; i < numItems; i++) {
            hdata[i] = new HeapItem(i, data[i]);
        }
        Heap hp = new Heap(hdata);
        findResidue(hp);
        // now hp has only one item, the residue
        return hp.remove().getPriority();
    }

    private static void findResidue(Heap hp) {
        long largest, secondLargest;
        HeapItem temp;
        // need to stick in something as the index of a combined item
        int index = hp.size(); 
        while (hp.size() > 1) {
            largest = hp.remove().getPriority();
            secondLargest = hp.remove().getPriority();
            long difference = largest - secondLargest;
            if (difference < 0)
                throw new RuntimeException ("Negative difference found");

            // technically we could skip this if difference == 0
            // but doing so would require a more complicated while condition
            temp = new HeapItem(index++, difference);
            hp.insert(temp);
        }
    }
}
