package com.architecting.ch11;

import java.util.Random;

public class XMLGenerator {

  public static Random random = new Random(System.currentTimeMillis());

  public static String getFirstName (int SIN) {
    int indexFirstName = (SIN & 0xFF) % 10;
    switch (indexFirstName) {
      case 0: return "Bob";
      case 1: return "Mike";
      case 2: return "Steve";
      case 3: return "Rob";
      case 4: return "Peter";
      case 5: return "Dan";
      case 6: return "Luck";
      case 7: return "Bryan";
      case 8: return "Dave";
      case 9: return "Eric";
    }
    return ""; // This should never occurs.
  }

  public static String getLastName (int SIN) {
    int indexLastName = ((SIN >> 16) & 0xFF) % 10;
    switch (indexLastName) {
      case 0: return "Garcia";
      case 1: return "Rodriguez";
      case 2: return "Smith";
      case 3: return "Miller";
      case 4: return "Powell";
      case 5: return "Olson";
      case 6: return "Field";
      case 7: return "";
      case 8: return "";
      case 9: return "";
    }
    return ""; // This should never occurs.
  }

  public static int getRandomSIN() {
    return random.nextInt();
  }

  public static void main(String[] args) {
    System.out.println (getLastName(getRandomSIN()));
  }

}
