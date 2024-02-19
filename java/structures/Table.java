import java.nio.ByteBuffer;

public class Table {
    private String name;
    private int number;
    private Attribute[] attributes;

    public Table(String name, int number, Attribute[] attributes) {
        this.name = name;
        this.number = number;
        this.attributes = attributes;
    }

    public String toString() {
        return null;
    }

    public int totalBytes(){
        int size = 0; // total bytes for binary representation
        size += Integer.BYTES + name.length() * 2; // name string w/ string length
        size += Integer.BYTES * 2; // ints number and length of attributes
        for (Attribute attribute : attributes) {
            size += attribute.totalBytes();
        }
        return size;
    }

    public void fillBytes(ByteBuffer buffer){
        buffer.putInt(name.length() * 2);
        for (char c : name.toCharArray()) {
            buffer.putChar(c);
        }
        buffer.putInt(number);
        buffer.putInt(attributes.length);
        for (Attribute attribute : attributes) {
            attribute.fillBytes(buffer);
        }
    }

    public Attribute[] getAttributes() {
        return attributes;
    }

    public String getName() {
        return name;
    }

    public int getNumber() {
        return number;
    }
}