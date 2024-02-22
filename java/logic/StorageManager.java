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
    public Catalog catalog;
    public List<Table> tables;

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
                List<Attribute> attributes = table.getAttributes();
               
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
        List<Record> allRecords = new ArrayList<>();

        Table table = catalog.getTables().get(tableNumber); 

        if (table == null) {
            return allRecords;
        }

        int pageCount = table.getPagecount();

        for (int i = 0; i < pageCount; i++) {
            Page page = buffer.read(table.getName(), i);

            if (page == null) {
                continue;
            }

            allRecords.addAll(page.getRecords());
        }

        return allRecords;
    }


    public void insertRecord_table (Table table, List<Record> recordInsert){
        if(table.getPagecount() == 0){
            String fileName = table.getNumber() + ".txt"; // Checks the table object  to determine its number and 
                                                          // creates a new file name with that number 
            File myObj = new File(fileName);
            
            Page page = new Page(table, recordInsert, 0);
            
            try (FileOutputStream fos = new FileOutputStream(myObj);
                    ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                    oos.writeObject(page);
                    oos.flush();
                    }
        }

        else{
            int pageCount = table.getPagecount();
    
            for (int i = 0; i < pageCount; i++) {
                Page page = buffer.read(table.getName(), i);
                List<Record> records = page.getRecords();
    
                for (Record record : records) {
                    List<Attribute> attributes = table.getAttributes();
               
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
                        List<Object> value = ((Record) recordInsert).getValues();
                        if (values.get(primaryKeyIndex).equals(value.get(primaryKeyIndex))) {
                            records.add((Record) recordInsert);
                        }
                }
                if(i == (pageCount - 1)){
                    records.add((Record)recordInsert);
                }
            }
        }
        }
    }
    
    public void deleteRecord_primarykey(Table table, Object primaryKey) {
        int pageCount = table.getPagecount();

        for (int i = 0; i < pageCount; i++) {
            Page page = buffer.read(table.getName(), i);
            List<Record> records = page.getRecords();
    
            for (Record record : records) {
                List<Attribute> attributes = table.getAttributes();
               
                int primaryKeyIndex = -1;
                for (int j = 0; j < attributes.size(); j++) {
                    Attribute attribute = attributes.get(j);
                    if (attribute.isKey()) {
                        primaryKeyIndex = j;
                    }
                }
    
                if (primaryKeyIndex != -1) {
                    List<Object> values = record.getValues();
                    if (values.get(primaryKeyIndex).equals(primaryKey)) {
                        records.remove(record);
                        return;
                    }
                }
            }
        }
    }
    public void updateRecord_primarykey(Table template, Object pKey, Record record){
        
    }
    public Table createTable(String name, int number, ArrayList<Attribute> TableAttr) {
        Table New_Table = new Table(name,number,TableAttr);
        return New_Table;
    }
    
    public void dropTable(String tablenamePassed){  
        for (int i = 0; i < tables.size(); i++) {
            Table tableSelected = tables.get(i);
            if (tableSelected.getName().equals(tablenamePassed)) {
                tables.remove(i);
            }  
    }
    }

    public void alterTable(String tableName, ArrayList<Attribute> newAttributes) {
        for (Table table : tables) {
            if (table.getName().equals(tableName)) {
                table.setAttributes(newAttributes);
            }
        }
    }

    
}