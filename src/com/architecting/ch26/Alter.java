package com.architecting.ch26;

public class Alter {
  public static void main(String[] args) {
      // Call of the alter asynchronous commands
      AlterAsync.main(args);
      // Wait for all the alterations to complete
      AlterStatus.main(args);
  }
}
