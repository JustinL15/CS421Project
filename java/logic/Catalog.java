import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Catalog {
    private int bufferSize;
    private int pageSize;
    private List<Table> tables;
    private HashMap<String, Integer> tableMap;

    public Catalog(int bufferSize, int pageSize, List<Table> tables) {
        this.bufferSize = bufferSize;
        this.pageSize = pageSize;
        this.tables = tables;
        this.tableMap = new HashMap<>();
        for (int i = 0; i < tables.size(); i++) {
            this.tableMap.put(tables.get(i).getName(), i);
        }
    }

    // creates a Catalog by deserializing a ByteBuffer
    public Catalog(int bufferSize, ByteBuffer buffer) {
        this.bufferSize = bufferSize;
        this.pageSize = buffer.getInt();
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
        tables.add(new Table("test", 0, atts));
        Catalog catalog = new Catalog(1, 1, tables);
        System.out.println(catalog);
        byte[] bytes = catalog.toBinary();
        System.out.println(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Catalog catalogTheseus = new Catalog(1, buffer);
        System.out.println(catalogTheseus);
    }
}