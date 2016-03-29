package com.architecting.ch09;

import java.util.Random;

public class XMLGenerator {

  public static Random random = new Random(System.currentTimeMillis());

  public static String getFirstName (long SIN) {
    if (random.nextInt(5) == 0) return "";
    int indexFirstName = ((int)(SIN & 0xFF)) % 10;
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

  public static String getLastName (long SIN) {
    if (random.nextInt(5) == 0) return "";
    int indexLastName = ((int)(SIN >> 16) & 0xFF) % 10;
    switch (indexLastName) {
      case 0: return "Smith";
      case 1: return "Johnson";
      case 2: return "Williams";
      case 3: return "Brown";
      case 4: return "Jones";
      case 5: return "Miller";
      case 6: return "Davis";
      case 7: return "Garcia";
      case 8: return "Rodriguez";
      case 9: return "Wilson";
    }
    return ""; // This should never occurs.
  }

  public static int getRandomSIN() {
    // The smaller the range is, the more changes you have to have more
    // than one document per patient.
    return  random.nextInt(10);
  }

  public static String getRandomMessage () {
    long SIN = getRandomSIN();
    StringBuffer buffer = new StringBuffer(200);
    buffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ClinicalDocument><PatientRecord><SIN>");
    buffer.append(SIN);
    buffer.append("</SIN><FirstName>");
    buffer.append(getFirstName(SIN));
    buffer.append("</FirstName><LastName>");
    buffer.append(getLastName(SIN));
    buffer.append("</LastName></PatientRecord><MedicalRecord><Comments>");
    // Put comment generation here.
    buffer.append("This is a comment");
    buffer.append("</Comments></MedicalRecord></ClinicalDocument>");
    return buffer.toString();
  }

  public static void main(String[] args) {
    System.out.println (getRandomMessage());
  }

}
