package com.architecting.ch25;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class AlterAsync {
  public static void main(String[] args) {
    // Instantiate default HBase configuration object.
    // Configuration file must be in the classpath
    Configuration config = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(config);
    	      // Instantiate HBase admin object using the configuration file.
      Admin admin = connection.getAdmin()) {

      // Create a new column descriptor for the family we want to add.
      HColumnDescriptor columnDescriptor = new HColumnDescriptor("f2");

      columnDescriptor.setMaxVersions(10);

      // Add a new column family to an existing table.
     // admin.addColumn("testtable", columnDescriptor);

      // Delete the newly added column family.
     // admin.deleteColumn("testtable", columnDescriptor.getNameAsString());

      // Retrieve current table descriptor
      HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("testtable"));

      // Add MAXFILESIZE configuration to current descriptor
      tableDescriptor.setConfiguration("MAX_FILESIZE", "21474836480");

      // Apply new updated descriptor to the table
      long time1 = System.currentTimeMillis();
    //  admin.modifyTable("testtable", tableDescriptor);
      System.out.println (System.currentTimeMillis() - time1);

      // Retrieve current table descriptor
      tableDescriptor = admin.getTableDescriptor(TableName.valueOf("testtable"));

      // Add MAXFILESIZE configuration to current descriptor
      tableDescriptor.removeConfiguration("MAX_FILESIZE");

      // Apply new updated descriptor to the table
     // admin.modifyTable("testtable", tableDescriptor);


      // Add a coprocessor to the table.
      String className = "org.hbasecookbook.MyRegionObserver";
      Path jarPath = new Path("/my.jar");
      Map<String, String> kvs = new HashMap<String, String>(2);
      kvs.put("email1", "my@email.com");
      kvs.put("email2", "other@email.com");
      tableDescriptor.addCoprocessor(className, jarPath, 123, kvs);

      // Apply new updated descriptor to the table
     // admin.modifyTable("testtable", tableDescriptor);

      // Remove coprocessor from the table.
      tableDescriptor.removeCoprocessor(className);
      // Apply new updated descriptor to the table
    //  admin.modifyTable("testtable", tableDescriptor);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
