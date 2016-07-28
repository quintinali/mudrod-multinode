package MUDROD.Datamining.tools;

import java.io.*;
import java.util.*;

public class NumericDataReader {

    private static int DEFAULT_NUM = 100;
    /** 
     * 
     */ 
    public static void main(String[] args) throws IOException {
        int numItems = DEFAULT_NUM;

        if (args.length > 1) {
            try {
                numItems = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("numItems must be an integer: " + args[1]);
                System.exit(2);
            }
        }
        long[] data = arrayFromFile(args[0], numItems);

    }

    public static long[] arrayFromFile(String filename, int numItems) {
        FileReader in = null;               
        in = openInputFile(filename);
        Scanner sc = new Scanner(in);

        long[] data = new long[numItems];

        int check = 0; // to check number of values
        check = read_data(sc, data);
        if (check < numItems)
            System.out.println("Warning: input file has fewer than " +
                                numItems + " values");

        sc.close();
        try {
            in.close();
        } catch (IOException e) {
            System.out.println("Can't close file " + filename);
            System.exit(1);
        }
        return data;
    }

    private static int read_data(Scanner sc, long[] data) {
        int i = 0, cnt = 0;
        while(sc.hasNextLong() && i < data.length) {
                data[i++] = sc.nextLong();
                cnt++;
        }
        return cnt;
    }

    /**
     * openInputFile - create a FileReader object for filename
     */
    private static FileReader openInputFile(String filename) {
        FileReader in = null;
        try {
            in = new FileReader(filename);
            return in;
        } catch (FileNotFoundException e) {
            System.out.println("Can't open file " + filename);
            System.exit(1);
        }
        return in;
    }

}

