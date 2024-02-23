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
    public String databaseLocation;

    public StorageManager(Catalog catalog, String databaseLocation){
        this.catalog = catalog;
        this.databaseLocation = databaseLocation;
        this.buffer = new Buffer(catalog, databaseLocation);

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
        Table New_Table = new Table(name,number,TableAttr, 0);
        return New_Table;
    }
    
    public void dropTable(String tablenamePassed) { 
        int droptableNum = catalog.getTableByName(tablenamePassed).getNumber();
        int moveTableNum = catalog.getTables().size() - 1;
        File fileToDelete = new File(databaseLocation + File.separator + "tables" + File.separator + droptableNum);
        File fileToRename = new File(databaseLocation + File.separator + "tables" + File.separator + moveTableNum);
        if (fileToDelete.exists() && fileToRename.exists() && fileToDelete.delete()) {
            catalog.dropTable(tablenamePassed);
            fileToRename.renameTo(new File(databaseLocation + File.separator + "tables" + File.separator + droptableNum));
        } else {
            System.err.println("Deletion error for file: " + tablenamePassed);
        }

    }

    public void alterTable(String tableName, ArrayList<Attribute> newAttributes) {
        List<Table> tables = catalog.getTables();
        for (Table table : tables) {
            if (table.getName().equals(tableName)) {
                table.setAttributes(newAttributes);
            }
        }
    }

    public boolean checkUnique(Table table, int attrCol, int value) {
        for (int i = 0; i < table.getPagecount(); i++) {
            Page page = buffer.read(table.getName(), i);
            for (Record r : page.getRecords()) {
                int compare = (int) r.getValues().get(attrCol);
                if (compare == value) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkUnique(Table table, int attrCol, double value) {
        for (int i = 0; i < table.getPagecount(); i++) {
            Page page = buffer.read(table.getName(), i);
            for (Record r : page.getRecords()) {
                double compare = (double) r.getValues().get(attrCol);
                if (compare == value) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkUnique(Table table, int attrCol, char[] value) {
        for (int i = 0; i < table.getPagecount(); i++) {
            Page page = buffer.read(table.getName(), i);
            for (Record r : page.getRecords()) {
                char[] compare = (char[]) r.getValues().get(attrCol);
                if (Arrays.equals(compare, value)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkUnique(Table table, int attrCol, boolean value) {
        for (int i = 0; i < table.getPagecount(); i++) {
            Page page = buffer.read(table.getName(), i);
            for (Record r : page.getRecords()) {
                boolean compare = (boolean) r.getValues().get(i);
                if (compare == value) {
                    return false;
                }
            }
        }
        return true;
    }
    
}