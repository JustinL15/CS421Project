import java.nio.ByteBuffer;

public class Catalog {
    private int buffer_size;
    private int page_size;
    private Table[] tables;

    public Catalog(int buffer_size, int page_size, Table[] tables) {
        this.buffer_size = buffer_size;
        this.page_size = page_size;
        this.tables = tables;
    }

    // creates a Catalog by deserializing a ByteBuffer
    public Catalog(ByteBuffer buffer, int buffer_size) {
        this.buffer_size = buffer_size;
        this.page_size = buffer.getInt();
        int tables_length = buffer.getInt();
        this.tables = new Table[tables_length];
        for (int i = 0; i < tables_length; i++) {
            this.tables[i] = new Table(buffer);
        }
    }

    public String toString() {
        String string = "";
        string += "buffer size: " + this.buffer_size + "\n";
        string += "page size: " + this.page_size + "\n";
        string += "tables:\n";
        for (Table table : tables) {
            string += table.toString();
        }
        return string;
    }

    private int totalBytes() {
        int size = 0;
        size += Integer.BYTES * 2; // int page_size and length of tables
        for (Table table : tables) {
            size += table.totalBytes();
        }
        return size;
    }

    private void writeBytes(ByteBuffer buffer) {
        buffer.putInt(page_size);
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