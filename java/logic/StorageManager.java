import java.io.BufferedReader;
import java.io.File;  // Import the File class
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;  // Import the IOException class to handle errors
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.print.DocFlavor.BYTE_ARRAY;

public class StorageManager {
    public Buffer buffer;

    public StorageManager(Buffer buffer){
        this.buffer = buffer;

    }
    // Buffer requires the Catalog and databaseLocation. consider creating Buffer in Main and passing it to a constructor.

    public Record getRecordByPrimaryKey(Table table, Object primaryKey) {
        int pageCount = table.getPagecount();
    
        for (int i = 0; i < pageCount; i++) {
            Page page = buffer.read(table.getName(), i);
            List<Record> records = page.getRecords();
    
            for (Record record : records) {
                List<Attribute> attributes = Arrays.asList(table.getAttributes());
               
                int primaryKeyIndex = -1;
                for (int j = 0; j < attributes.size(); j++) {
                    Attribute attribute = attributes.get(j);
                    if (attribute.isKey()) {
                        primaryKeyIndex = j;
                        break;
                    }
                }
    
                if (primaryKeyIndex != -1) {
                    List<Object> values = record.getValues();
                    if (values.get(primaryKeyIndex).equals(primaryKey)) {
                        return record;
                    }
                }
            }
        }
        return null;
    }
    
    public Page getPage(String tableName, int pageNumber) {
        return buffer.read(tableName, pageNumber);
    }

    public List<Record> getRecords_tablenumber(int tableNumber) {
        Table[] tables = buffer.getCatalog().getTables();
        Table table = null;
        for (Table t : tables) {
            if (t.getNumber() == tableNumber) {
                table = t;
                break;
            }
        }
        if (table != null) {
            return buffer.getRecordsByTableNumber(tableNumber);
        }
        return null; // Table not found
    }

    public void insertRecord_table (Table table, List<Record> record){
        if(table.getPagecount() == 0){
            String fileName = table.getNumber() + ".txt"; // Checks the table object  to determine its number and 
                                                          // creates a new file name with that number 
            File myObj = new File(fileName);
            Page page = new Page(table, record, 1);
            
            try (FileOutputStream fos = new FileOutputStream(myObj);
                    ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                    oos.writeObject(page);
                    oos.flush();
                    }
        }

        else{
            int b = 0;
            Attribute[] attributes = table.getAttributes();
            Boolean key;
            for (int i = 0; i < attributes.length; i++) {
                Attribute holding = attributes[i];
                if (holding.isKey()) {
                    key = holding.isKey();
                    break;
                }
            }
            while(true){
                Page page = buffer.read(table.getName(), b);
                List<Record> cur_Records = page.getRecords();
                int a = 0;
                while(true){
                    Record latest = cur_Records.get(a);
                    if(){

                    }
                }
                    if(/* Page becomes overfull */){
                        // Split the page, then end the function
                    }
                b += 1;
            }
        }
        if(/* Record is not inserted */){
            //Insert into last page, if overfull split and end the function
        }
    }
    
    public void deleteRecord_primarykey(){

    }
    public void updateRecord_primarykey(Table template, Object pKey, Record record){
        
    }
    public void createTable(Table new_Table) {
    }
    
    public void dropTable(String name){    
    }
    
}