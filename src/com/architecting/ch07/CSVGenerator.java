package com.architecting.ch07;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class CSVGenerator {
  
  public static final Random random = new Random(System.currentTimeMillis());

  public static int id = 0;
  public static String generateId() {
    // Keep only the 4 first bytes to have some duplicate IDs
    // which will simulate extra columns.
    return UUID.randomUUID().toString().substring(0, 4);
  }

  public static String generateEventId() {
    return UUID.randomUUID().toString();
  }

  public static String generateDocType() {
    switch (random.nextInt(4)) {
      case 0: return "ALERT";
      case 1: return "WARNING";
      case 2: return "FAILURE";
      case 3: return "RETURNED";
      default: return null;
    }
  }

  public static String generatePartName () {
    return "NE-" + random.nextInt(1000);
  }
  
  public static final StringBuffer buffer = new StringBuffer(1024);
  public static String generatePayLoad () {
	  buffer.setLength(0);
	  int length = 64 + random.nextInt(64);
	  for (int i = 0; i < length; i++) {
		  buffer.append((char)(65 + random.nextInt(26)));
	  }
	  buffer.append(" ");
	  return buffer.toString();
  }

  public static String generatePartNumber () {
    return random.nextInt(1) +
           random.nextInt(1) +
           random.nextInt(1) +
           "-" +
           random.nextInt(1) +
           random.nextInt(1) +
           random.nextInt(1) +
           random.nextInt(1) +
           "-" +
           random.nextInt(1) +
           random.nextInt(1) +
           random.nextInt(1);
  }


  public static void main(String[] args) {
	System.out.println ("Generaging...");
    //BufferedWriter bw = null;
    BufferedWriter bw;
    try {
      bw = new BufferedWriter (new FileWriter("./resources/ch09/omneo.csv"));
      for (int index = 0; index < 1000000; index++) {
        String id = generateId();
        bw.write(id);
        bw.write(",");
        bw.write(generateEventId());
        bw.write(",");
        bw.write(generateDocType());
        bw.write(",");
        bw.write(generatePartName());
        bw.write(",");
        bw.write(generatePartNumber());
        bw.write(",");
        bw.write("1");
        bw.write(",");
        bw.write(generatePayLoad() + id);
        bw.newLine();
      }
      bw.flush();
      bw.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("Done");
  }

}
