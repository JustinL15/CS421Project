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
        // TODO if indexing is on, then use the tree to find the record
        if (true) {
            // int[] recordPointer = ((BPlusTreeNode)buffer.read(table.getName(), 0, true)).search((Integer)primaryKey);
            int[] recordPointer = new int[2];
            Page page = (Page)buffer.read(table.getName(), recordPointer[0], false);
            List<Record> records = page.getRecords();
            return records.get(recordPointer[1]);
        }
        int pageCount = table.getPagecount();
    
        for (int i = 0; i < pageCount; i++) {
            Page page = (Page)buffer.read(table.getName(), i, false);
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
        return (Page)buffer.read(tableName, pageNumber, false);
    }

    public List<Record> getRecords_tablenumber(int tableNumber) {
        List<Record> allRecords = new ArrayList<>();

        Table table = catalog.getTables().get(tableNumber); 

        if (table == null) {
            return allRecords;
        }

        int pageCount = table.getPagecount();

        for (int i = 0; i < pageCount; i++) {
            Page page = (Page)buffer.read(table.getName(), i, false);

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

    public void insertSingleRecord(Table table, Record newRecord) throws Exception {
        if (newRecord.spacedUsed() + 4 > catalog.getPageSize()) {
            throw new Exception("Record size larger than page size");
        }
        // TODO if indexing is on
        if (true) {
            BPlusTreeNode tree = (BPlusTreeNode)buffer.read(table.getName(), 0, true);
            BPlusTreeNode leaf = tree.insert((int)newRecord.getPrimaryKey(), null, buffer);
            int index = BPlusTreeNode.binarySearch(leaf.getKeys(), (int)newRecord.getPrimaryKey());
            int[] recordPointer = leaf.getPointers().get(index);
            Page page = (Page)buffer.read(table.getName(), recordPointer[0], false);
            List<Record> records = page.getRecords();
            records.add(recordPointer[1], newRecord);
            return;
        }
        if (table.getPagecount() == 0) {
            Page page = (Page)buffer.read(table.getName(), 0, false);
            page.getRecords().add(newRecord);
            table.setPageCount(1);
            return;
        } else {
            for (int i = 0; i < table.getPagecount(); i++) {
                Page page = (Page)buffer.read(table.getName(), i, false);
                List<Record> records = page.getRecords();
                if (records.size() == 0) {
                    records.add(newRecord);
                    return;
                }
                for (int j = 0; j < records.size(); j++) {
                    if (insertRecord_table_helper(table, newRecord, records.get(j))) {
                        page.getRecords().add(j, newRecord);
                        return;
                    }
                }
                if (i + 1 == table.getPagecount()) {
                    page.getRecords().add(newRecord);
                    return;
                }
            }
        }
    }

    
    public void deleteRecord_primarykey(Table table, Object primaryKey) throws Exception {
        // TODO if indexing is on
        if (true) {
            BPlusTreeNode tree = (BPlusTreeNode)buffer.read(table.getName(), 0, true);
            // int[] recordPointer = tree.search((int)primaryKey);
            int[] recordPointer = new int[2];
            Page page = (Page)buffer.read(table.getName(), recordPointer[0], false);
            List<Record> records = page.getRecords();
            records.remove(recordPointer[1]);
            tree.delete((int)primaryKey, buffer, null, null);
            return;
        }
        int pageCount = table.getPagecount();

        for (int i = 0; i < pageCount; i++) {
            Page page = (Page)buffer.read(table.getName(), i, false);
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
    public void updateRecord_primarykey(Object pKey, Record record) throws Exception{
        deleteRecord_primarykey(record.getTemplate(), pKey);
        insertSingleRecord(record.getTemplate(), record);
    }
    public Table createTable(String name, int number, List<Attribute> TableAttr) {
        Table New_Table = new Table(name,number,TableAttr, 0);
        catalog.createTable(New_Table);
        // TODO if indexing is on, create tree
        if (true) {
            buffer.read(name, 0, true);
        }
        return New_Table;
    }
    
    public void dropTable(String tablenamePassed) { 
        int droptableNum = catalog.getTableByName(tablenamePassed).getNumber();
        int moveTableNum = catalog.getTables().size() - 1;
        File fileToDelete = new File(databaseLocation + File.separator + "tables" + File.separator + droptableNum);
        File fileToRename = new File(databaseLocation + File.separator + "tables" + File.separator + moveTableNum);
        catalog.dropTable(tablenamePassed);
        if (fileToDelete.exists()) {
            fileToDelete.delete();
        }
        if (fileToRename.exists()) {
            fileToRename.renameTo(new File(databaseLocation + File.separator + "tables" + File.separator + droptableNum));
        }
        
        // TODO if indexing is on, delete tree
        if (true) {
            fileToDelete = new File(databaseLocation + File.separator + "trees" + File.separator + droptableNum);
            fileToRename = new File(databaseLocation + File.separator + "trees" + File.separator + moveTableNum);
            if (fileToDelete.exists()) {
                fileToDelete.delete();
            }
            if (fileToRename.exists()) {
                fileToRename.renameTo(new File(databaseLocation + File.separator + "trees" + File.separator + droptableNum));
            }
        }

    }

    public int checkUnique(Table table, int attrCol, Object value) {
        switch (table.getAttributes().get(attrCol).getDataType()) {
            case Integer:
                int ival = (int) value;
                for (int i = 0; i < table.getPagecount(); i++) {
                    Page page = (Page)buffer.read(table.getName(), i, false);
                    for (Record r : page.getRecords()) {
                        int compare = (int) r.getValues().get(attrCol);
                        if (compare == ival) {
                            return i;
                        }
                    }
                }
                break;
            case Double:
                double dval = (double) value;
                for (int i = 0; i < table.getPagecount(); i++) {
                    Page page = (Page)buffer.read(table.getName(), i, false);
                    for (Record r : page.getRecords()) {
                        double compare = (double) r.getValues().get(attrCol);
                        if (compare == dval) {
                            return i;
                        }
                    }
                }
                break;
            case Boolean:
                boolean bval = (boolean) value;
                for (int i = 0; i < table.getPagecount(); i++) {
                    Page page = (Page)buffer.read(table.getName(), i, false);
                    for (Record r : page.getRecords()) {
                        boolean compare = (boolean) r.getValues().get(attrCol);
                        if (compare == bval) {
                            return i;
                        }
                    }
                }
                break;
            case Char:
            case Varchar:
                String val = (String) value;
                for (int i = 0; i < table.getPagecount(); i++) {
                    Page page = (Page)buffer.read(table.getName(), i, false);
                    for (Record r : page.getRecords()) {
                        String compare = (String) r.getValues().get(attrCol);
                        if ((compare.equals(val))) {
                            return i;
                        }
                    }
                }
                break;
        }
        return -1;
    }

    public void add_table_column(Table table, Attribute newAttr, Object defaultval) throws Exception {
        int pageCount = table.getPagecount();

        List<Record> recordsToUpdate = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            Page page = (Page)buffer.read(table.getName(), i, false);
            List<Record> records = page.getRecords();
            for (Record record : records) {  
                List<Object> recordvals = record.getValues();
                recordvals.add(defaultval);
                record.setTemplate(table);
            }
            recordsToUpdate.addAll(records);
        }
        buffer.purge();
        buffer.cleanTable(table);
        table.setPageCount(0);
        for (Record r: recordsToUpdate) {
            insertSingleRecord(table, r);
        }
        List<Attribute> attrlist =  table.getAttributes();
        attrlist.add(newAttr);
        table.setAttributes(attrlist);
    }

    public void delete_table_column(Table table, String deleteAttribute, int index) {
        List<Attribute> attrlist =  table.getAttributes();
        attrlist.remove(index);
        // table.setAttributes(attrlist);
        int pageCount = table.getPagecount();

        for (int i = 0; i < pageCount; i++) {
            Page page = (Page)buffer.read(table.getName(), i, false);
            List<Record> records = page.getRecords();
            for (Record record : records) {
                List<Object> recordvals = record.getValues();
                recordvals.remove(index);
                // record.setTemplate(table);
            }
        }
 
 
    }

    public static void main(String[] args) throws Exception {
        Catalog catalog = new Catalog(4, 40, new ArrayList<Table>());
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
        System.out.println(nw.spacedUsed());
        // for (int i = 0; i < 8; i++) {
        //     System.out.println(i);
        // }
        for (int i = 0; i < table.getPagecount(); i++) {
            System.out.println("Page " + i);
            Page page = (Page)sM.buffer.read(table.getName(), i, false);
            for (Record record : page.getRecords()) {
                System.out.println(record.getValues());
            }
        }
        System.out.println("\n");

        sM.insertSingleRecord(table, nw);
        for (int i = 0; i < table.getPagecount(); i++) {
            System.out.println("Page " + i);
            Page page = (Page)sM.buffer.read(table.getName(), i, false);
            System.out.println(page.getRecords().size());
            for (Record record : page.getRecords()) {
                System.out.println(record.getValues());
            }
        }
        System.out.println("\n");

        List<Object> newVals = new ArrayList<>(vals);
        newVals.set(0, 2);
        sM.insertSingleRecord(table, new Record(table, newVals));
        for (int i = 0; i < table.getPagecount(); i++) {
            System.out.println("Page " + i);
            Page page = (Page)sM.buffer.read(table.getName(), i, false);
            for (Record record : page.getRecords()) {
                System.out.println(record.getValues());
            }
        }
        System.out.println("\n");

        List<Object> newerVals = new ArrayList<>(vals);
        newerVals.set(0, 3);
        sM.insertSingleRecord(table, new Record(table, newerVals));
        // Page page = sM.buffer.read(table.getName(), 0);
        // Page page1 = sM.buffer.read(table.getName(), 1);
        // for (Record record : page.getRecords()) {
        //     System.out.println(record.getValues());
        // }
        // System.out.println("new page\n");
        // for (Record record : page1.getRecords()) {
        //     System.out.println(record.getValues());
        // }
        for (int i = 0; i < table.getPagecount(); i++) {
            System.out.println("Page " + i);
            Page page = (Page)sM.buffer.read(table.getName(), i, false);
            for (Record record : page.getRecords()) {
                System.out.println(record.getValues());
            }
        }
    }
}