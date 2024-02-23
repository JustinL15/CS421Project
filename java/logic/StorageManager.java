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

    public boolean insertRecord_table_helper(Table table, Record newRecord, Record record) {
        int pkIndex = -1;
        for (int i = 0; i < table.getAttributes().size(); i++) {
            if (table.getAttributes().get(i).isKey()) {
                pkIndex = i;
                break;
            }
        }
        switch(table.getAttributes().get(pkIndex).getDataType()) {
            case Integer:
                return (int) record.getValues().get(pkIndex) > (int) newRecord.getValues().get(pkIndex); 
            case Double:
                return (double) record.getValues().get(pkIndex) > (double) newRecord.getValues().get(pkIndex);
            case Boolean:
                int eqvOld = (boolean) record.getValues().get(pkIndex) ? 1 : 0;
                int eqvNew = (boolean) newRecord.getValues().get(pkIndex) ? 1 : 0;
                return eqvOld > eqvNew;
            case Char:
            case Varchar:
                String oldString = (String) record.getValues().get(pkIndex);
                String newString = (String) newRecord.getValues().get(pkIndex);
                int compareVal = oldString.compareTo(newString);
                return compareVal > 0;
            default:
                return false;
        }
    }


    public void insertRecord_table (Table table, List<Record> newRecords){
        if(table.getPagecount() == 0){
            Page page = buffer.read(table.getName(), 0);
            page.getRecords().addAll(newRecords);
        }

        else{
            int pageCount = table.getPagecount();
    
            List<Attribute> attributes = table.getAttributes();
       
            int primaryKeyIndex = -1;
            for (int j = 0; j < attributes.size(); j++) {
                Attribute attribute = attributes.get(j);
                if (attribute.isKey()) {
                    primaryKeyIndex = j;
                    break;
                }
            }

            for (Record newRecord : newRecords) {
                for (int i = 0; i < pageCount; i++) {
                    Page page = buffer.read(table.getName(), i);
                    List<Record> records = page.getRecords();
        
                    for (Record record : records) {

                        List<Object> values = record.getValues();
                        List<Object> newValues = newRecord.getValues();
                        if (values.get(primaryKeyIndex).equals(newValues.get(primaryKeyIndex))) {
                            System.out.println("Duplicate primary key " + newValues);
                            return;
                        }
                        if (insertRecord_table_helper(table, newRecord, record)) {
                            if ((page.bytesUsed() + newRecord.spacedUsed()) > page.bytesUsed()){
                                buffer.splitPage(databaseLocation, i);
                                records.add(newRecord);
                            }
                            else{
                                records.add(newRecord);
                            }
                        }
                        if(i == (pageCount - 1)){ // this condition isn't right
                            if ((page.bytesUsed() + newRecord.spacedUsed()) > page.bytesUsed()){
                                buffer.splitPage(databaseLocation, i);
                                records.add(newRecord);
                            }
                            else{
                                records.add(newRecord);
                            }
                        }
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
    public Table createTable(String name, int number, List<Attribute> TableAttr) {
        Table New_Table = new Table(name,number,TableAttr, 0);
        catalog.getTables().add(New_Table);
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
    

    public void add_table_column(Table table, Attribute newAttr, Object defaultval) {
        List<Attribute> attrlist =  table.getAttributes();
        attrlist.add(newAttr);
        table.setAttributes(attrlist);
        int pageCount = table.getPagecount();
        int mysize = newAttr.getbytesize(defaultval);


        for (int i = 0; i < pageCount; i++) {
            Page page = buffer.read(table.getName(), i);
            List<Record> records = page.getRecords();
            if(page.bytesUsed() + (records.size() * mysize) > catalog.getPageSize()){
                Page[] pages =buffer.splitPage(table.getName(), i);
                page = pages[0];
                records = page.getRecords();
            }
            for (Record record : records) {  
                List<Object> recordvals = record.getValues();
                recordvals.add(defaultval);
                record.setTemplate(table);
            }
        }
        
    }

    public void delete_table_column(Table table, String deleteAttribute, int index) {
        List<Attribute> attrlist =  table.getAttributes();
        
        table.setAttributes(attrlist);
        int pageCount = table.getPagecount();

        for (int i = 0; i < pageCount; i++) {
            Page page = buffer.read(table.getName(), i);
            List<Record> records = page.getRecords();
            for (Record record : records) {
                List<Object> recordvals = record.getValues();
                recordvals.remove(index);
                record.setTemplate(table);
            }
        }
 
 
    }

    public static void main(String[] args) {
        Catalog catalog = new Catalog(4, 1000, new ArrayList<Table>());
        StorageManager sM = new StorageManager(catalog, "resources");
        Attribute name = new Attribute("name", Type.Varchar, 10, false, false, false);
        Attribute age = new Attribute("age", Type.Integer, 0, false, false, false);
        Attribute netWorth = new Attribute("newWorth", Type.Double, 0, false, false, false);
        Attribute Id = new Attribute("id", Type.Integer, 0, false, true, false);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(Id);
        attributes.add(name);
        attributes.add(age);
        attributes.add(netWorth);
        sM.createTable("test", 0, attributes);
        System.out.println(catalog.getTables().get(0).getName());
        Table table = catalog.getTableByName("test");
        List<Object> vals = new ArrayList<>();
        vals.add(1);
        vals.add("John");
        vals.add(18);
        vals.add(90.01);
        Record nw = new Record(table, vals);
        List<Record> in = new ArrayList<>();
        in.add(nw);
        sM.In
    }
}