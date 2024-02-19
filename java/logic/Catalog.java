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

    public String toString() {
        return null;
    }

    private int totalBytes() {
        int size = 0;
        size += Integer.BYTES * 2; // int page_size and length of tables
        for (Table table : tables) {
            size += table.totalBytes();
        }
        return size;
    }

    private void fillBytes(ByteBuffer buffer) {
        buffer.putInt(page_size);
        buffer.putInt(tables.length);
        for (Table table : tables) {
            table.fillBytes(buffer);
        }
    }

    public byte[] toBinary() {
        int size = totalBytes();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        fillBytes(buffer);
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
        byte[] bytes = catalog.toBinary();
        System.out.println(bytes);
    }
}