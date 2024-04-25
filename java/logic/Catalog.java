import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Catalog {
    private int bufferSize;
    private int pageSize;
    private List<Table> tables;
    private HashMap<String, Integer> tableMap;
    private boolean BPlusindex;

    public Catalog(int bufferSize, int pageSize, List<Table> tables, boolean indexing) {
        this.bufferSize = bufferSize;
        this.pageSize = pageSize;
        this.tables = tables;
        this.BPlusindex = indexing;
        this.tableMap = new HashMap<>();
        for (int i = 0; i < tables.size(); i++) {
            this.tableMap.put(tables.get(i).getName(), i);
        }
    }

    // creates a Catalog by deserializing a ByteBuffer
    public Catalog(int bufferSize, ByteBuffer buffer,boolean indexing) {
        this.bufferSize = bufferSize;
        this.pageSize = buffer.getInt();
        this.BPlusindex = indexing;
        int tables_length = buffer.getInt();
        this.tables = new ArrayList<Table>();
        for (int i = 0; i < tables_length; i++) {
            this.tables.add(new Table(buffer));
        }
        this.tableMap = new HashMap<>();
        for (int i = 0; i < tables.size(); i++) {
            this.tableMap.put(tables.get(i).getName(), i);
        }
    }

    public String toString() {
        String string = "";
        string += "buffer size: " + this.bufferSize + "\n";
        string += "page size: " + this.pageSize + "\n";
        string += "indexing: " + this.BPlusindex + "\n";
        string += "tables:\n";
        for (Table table : tables) {
            string += table.toString();
        }
        
        return string;
    }

    private int totalBytes() {
        int size = 0;
        size += Integer.BYTES * 2; // int pageSize and length of tables
        for (Table table : tables) {
            size += table.totalBytes();
        }
        return size;
    }

    private void writeBytes(ByteBuffer buffer) {
        buffer.putInt(pageSize);
        buffer.putInt(tables.size());
        for (Table table : tables) {
            table.writeBytes(buffer);
        }
    }

    public byte[] toBinary() {
        int size = totalBytes();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        writeBytes(buffer);
        return buffer.array();
    }

    public List<Table> getTables() {
        return tables;
    }

    public Table getTableByName(String name) {
        Integer tableNumber = tableMap.get(name);
        if (tableNumber == null){
            return null;
        }
        return tables.get(tableNumber);
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public boolean getBPlusIndex(){
        return BPlusindex;
    }

    public void createTable(Table table) {
        tables.add(table);
        tableMap.put(table.getName(), table.getNumber());
    }

    public void dropTable(String tableName) {
        Table droppedTable = tables.get(tableMap.get(tableName));
        Table movedTable = tables.get(tables.size() - 1);

        movedTable.setNumber(droppedTable.getNumber());

        tableMap.put(movedTable.getName(), droppedTable.getNumber());
        tableMap.remove(tableName);

        tables.set(droppedTable.getNumber(), movedTable);
        tables.remove(tables.size() - 1);
    }

    public static void main(String[] args) {
        List<Attribute> atts = new ArrayList<>();
        atts.add(new Attribute("name", Type.Boolean, 1, true, true, true));
        atts.add(new Attribute("id", Type.Integer, 1, false, false, false));
        List<Table> tables = new ArrayList<>();
        tables.add(new Table("test", 0, atts,0));
        Catalog catalog = new Catalog(1, 1, tables,false);
        System.out.println(catalog);
        byte[] bytes = catalog.toBinary();
        System.out.println(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Catalog catalogTheseus = new Catalog(1, buffer,false);
        System.out.println(catalogTheseus);
    }
}