import java.nio.ByteBuffer;
import java.util.HashMap;

public class Catalog {
    private int bufferSize;
    private int pageSize;
    private Table[] tables;
    private HashMap<String, Integer> tableMap;

    public Catalog(int bufferSize, int pageSize, Table[] tables) {
        this.bufferSize = bufferSize;
        this.pageSize = pageSize;
        this.tables = tables;
        this.tableMap = new HashMap<>();
        for (int i = 0; i < tables.length; i++) {
            this.tableMap.put(tables[i].getName(), i);
        }
    }

    // creates a Catalog by deserializing a ByteBuffer
    public Catalog(ByteBuffer buffer, int bufferSize) {
        this.bufferSize = bufferSize;
        this.pageSize = buffer.getInt();
        int tables_length = buffer.getInt();
        this.tables = new Table[tables_length];
        for (int i = 0; i < tables_length; i++) {
            this.tables[i] = new Table(buffer);
        }
        this.tableMap = new HashMap<>();
        for (int i = 0; i < tables.length; i++) {
            this.tableMap.put(tables[i].getName(), i);
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
        buffer.putInt(tables.length);
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

    public Table[] getTables() {
        return tables;
    }

    public Table getTableByName(String name) {
        return tables[tableMap.get(name)];
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public static void main(String[] args) {
        Catalog catalog = new Catalog(1, 1, new Table[] {
                new Table("name", 0, new Attribute[] { new Attribute("name", Type.Boolean, 1, true, true, true) }),
                new Table("name", 0, new Attribute[] { new Attribute("name", Type.Boolean, 1, true, true, true) })
        });
        System.out.println(catalog);
        byte[] bytes = catalog.toBinary();
        System.out.println(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Catalog catalogTheseus = new Catalog(buffer, 1);
        System.out.println(catalogTheseus);
    }
}