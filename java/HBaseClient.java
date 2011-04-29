import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;


public class HBaseClient {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(config);
        String tableName = "tweets";
        String columnFamilyName = "raw";
        if (!doesTableExist(admin, tableName)) {
            createTable(admin, tableName, columnFamilyName);
        }

        HTable table = new HTable(config, tableName);
        byte[] key1 = Bytes.toBytes("id1");
        Put p1 = new Put(key1);
        String columnName = "value2";
        p1.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), Bytes.toBytes(new Date().toString()));
        table.put(p1);

        p1 = new Put(key1);
        p1.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes("value"), Bytes.toBytes("Leif was here"));
        table.put(p1);

        Get g = new Get(key1);
        Result result = table.get(g);
        System.out.println("Got: " + result);

        System.out.println("\nBeginning Scan");
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result scannerResult : scanner) {
            System.out.println("Scan Result: " + scannerResult);
            for (KeyValue kv : scannerResult.getColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName))) {
                System.out.println("    " + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier()) + "(" + kv.getTimestamp() + "): " + Bytes.toString(kv.getValue()));
            }
        }
    }

    private static boolean doesTableExist(HBaseAdmin admin, String tableName) throws Exception {
        byte[] tableBytes = tableName.getBytes();
        for (HTableDescriptor table : admin.listTables()) {
            if (Bytes.equals(tableBytes, table.getName())) {
                System.out.println("Found " + tableName + " " +  table);
                return true;
            }
        }
        System.out.println("Did not find " + tableName);
        return false;
    }

    private static void createTable(HBaseAdmin admin, String tableName, String columnFamilyName) throws Exception {
        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor column = new HColumnDescriptor(columnFamilyName);
        table.addFamily(column);
        admin.createTable(table);
        System.out.println("Created " + tableName + " with " + columnFamilyName);
    }
}
