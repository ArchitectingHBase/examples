package com.architecting.ch11;

import java.util.Random;

public class DataGenerator {

  protected static final Random random = new Random(System.currentTimeMillis());

  protected static final String[] fields = {"firstname", "lastname", "birthdate", "postalcode", "city", "sexe", "status"};
  protected static final String[] firstNames = {"Maurice", "Kevin", "Groseille", "Gauthier", "Gaelle", "Audrey", "Nicole"};
  protected static final String[] lastNames = {"Spaggiari", "O'Dell", "Berrouard", "Smith", "Johns", "Samuel", "Garbuet"};
  protected static final String[] birthDates = {"06/03/1942", "23/05/1965", "01/11/1977", "30/06/1998", "20/03/2000", "23/09/2002", "20/05/1946"};
  protected static final String[] postalCodes = {"34920", "05274", "H2M1N6", "H2M2V5", "34270", "H0H0H0", "91610"};
  protected static final String[] cities = {"Le Cres", "Miami", "Montreal", "Ottawa", "Maugio", "Cocagne", "Ballancourt"};
  protected static final String[] sexes = {"male", "male", "female", "male", "unknown", "female", "female"};
  protected static final String[] statuses = {"maried", "maried", "en couple", "en couple", "single", "en couple", "veuve"};

  /**
   * Generate an almost random UUID (data set one million).
   * @return
   */
  public static String generateUUDI() {
    StringBuffer value = new StringBuffer("" + random.nextInt(1000000));
    while (value.length() < 6)
      value.insert(0, "0");
    return "ec3ea672-3313-465d-bdb5-7a2453" + value.toString();
  }

  public static String generateFieldName() {
    return fields[random.nextInt(fields.length)];
  }

  public static String generateData(String fieldName, String uuid) {
    if (fieldName.equals("status")) {
      return statuses[Math.abs(uuid.hashCode()) % statuses.length];
    }
    if (fieldName.equals("sexe")) {
      return sexes[Math.abs(uuid.hashCode()) % sexes.length];
    }
    if (fieldName.equals("city")) {
      return cities[Math.abs(uuid.hashCode()) % cities.length];
    }
    if (fieldName.equals("postalcode")) {
      return postalCodes[Math.abs(uuid.hashCode()) % postalCodes.length];
    }
    if (fieldName.equals("birthdate")) {
      return birthDates[Math.abs(uuid.hashCode()) % birthDates.length];
    }
    if (fieldName.equals("lastname")) {
      return lastNames[Math.abs(uuid.hashCode()) % lastNames.length];
    }
    if (fieldName.equals("firstname")) {
      return firstNames[Math.abs(uuid.hashCode()) % firstNames.length];
    }
    return "unknow field type";
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Syntax: DataGenerator iterations_count");
      return;
    }
    long iterations = Long.parseLong(args[0]);
    for (int i = 0; i < iterations; i++) {
      String fieldName = generateFieldName();
      String uuid = generateUUDI();
      System.out.println (new StringBuilder(uuid).reverse().toString() + "|" + fieldName + "|" + generateData(fieldName, uuid));
    }
  }

}
