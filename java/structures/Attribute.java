import java.nio.ByteBuffer;

public class Attribute {
    private String name;
    private Type data_type;
    private int max_length;
    private boolean nullable;
    private boolean key;
    private boolean unique;

    public Attribute(String name, Type data_type, int max_length, boolean nullable, boolean key, boolean unique) {
        this.name = name;
        this.data_type = data_type;
        this.max_length = max_length;
        this.nullable = nullable;
        this.key = key;
        this.unique = unique;
    }

    public int totalBytes() {
        int size = 0; // total bytes for binary representation
        size += Integer.BYTES + name.length() * 2; // name string w/ string length
        size += Integer.BYTES * 2; // ints data_type and max_length
        size += 3; // booleans nullable, key, and unique
        return size;
    }

    public void fillBytes(ByteBuffer buffer) {
        buffer.putInt(data_type.ordinal());
        buffer.putInt(max_length);
        buffer.put(nullable ? (byte) 1 : (byte) 0);
        buffer.put(key ? (byte) 1 : (byte) 0);
        buffer.put(unique ? (byte) 1 : (byte) 0);
        buffer.putInt(name.length() * 2);
        for (char c : name.toCharArray()) {
            buffer.putChar(c);
        }
    }

    public String getName() {
        return this.name;
    }

    public Type getDataType() {
        return this.data_type;
    }

    public int getMaxLength() {
        return this.max_length;
    }
}