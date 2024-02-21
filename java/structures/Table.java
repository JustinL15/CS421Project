import java.nio.ByteBuffer;

public class Table {
    private String name;
    private int number;
    private Attribute[] attributes;
    private int pagecount;
    private int recordcount;

    public Table(String name, int number, Attribute[] attributes) {
        this.name = name;
        this.number = number;
        this.attributes = attributes;
    }

    // creates a Table by deserializing a ByteBuffer
    public Table(ByteBuffer buffer) {
        this.name = "";
        int name_length = buffer.getInt();
        for (int i = 0; i < name_length; i++) {
            this.name += buffer.getChar();
        }
        this.number = buffer.getInt();
        int attributes_length = buffer.getInt();
        this.attributes = new Attribute[attributes_length];
        for (int i = 0; i < attributes_length; i++) {
            this.attributes[i] = new Attribute(buffer);
        }
    }

    public String toString() {
        String string = "";
        string += "\ttable name: " + this.name + "\n";
        string += "\ttable number: " + this.number + "\n";
        string += "\tattributes:\n";
        for (Attribute attribute : this.attributes) {
            string += attribute.toString();
        }
        return string;
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

    public void writeBytes(ByteBuffer buffer){
        buffer.putInt(name.length());
        for (char c : name.toCharArray()) {
            buffer.putChar(c);
        }
        buffer.putInt(number);
        buffer.putInt(attributes.length);
        for (Attribute attribute : attributes) {
            attribute.writeBytes(buffer);
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
    public int getPagecount(){
        return this.pagecount;
    }
    public int getRecordcount(){
        return this.recordcount;
    }
}