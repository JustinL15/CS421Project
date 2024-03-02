import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Table {
    private String name;
    private int number;
    private List<Attribute> attributes;
    private int pagecount;

    public Table(String name, int number, List<Attribute> attributes, int pagecount) {
        this.name = name;
        this.number = number;
        this.attributes = attributes;
        this.pagecount = pagecount;
    }

    // creates a Table by deserializing a ByteBuffer
    public Table(ByteBuffer buffer) {
        this.name = "";
        int name_length = buffer.getInt();
        for (int i = 0; i < name_length; i++) {
            this.name += buffer.getChar();
        }
        this.number = buffer.getInt();
        this.pagecount = buffer.getInt();
        int attributes_length = buffer.getInt();
        this.attributes = new ArrayList<Attribute>();
        for (int i = 0; i < attributes_length; i++) {
            this.attributes.add(new Attribute(buffer));
        }
    }

    public String toString() {
        String string = "";
        string += "\ttable name: " + this.name + "\n";
        string += "\ttable number: " + this.number + "\n";
        string += "\tattributes:\n";
        for (Attribute attribute : this.attributes) {
            string += attribute.toString() + "\n";
        }
        return string;
    }

    public int totalBytes(){
        int size = 0; // total bytes for binary representation
        size += Integer.BYTES + name.length() * 2; // name string w/ string length
        size += Integer.BYTES * 3; // ints number, length of attributes, and page count
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
        buffer.putInt(pagecount);
        buffer.putInt(attributes.size());
        for (Attribute attribute : attributes) {
            attribute.writeBytes(buffer);
        }
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public String getName() {
        return name;
    }

    public int getNumber() {
        return number;
    }
    public void setNumber(int number) {
        this.number = number;
    }
    public int getPagecount(){
        return this.pagecount;
    }

    public void setPageCount(int number){
        this.pagecount = number;
    }

    public void setAttributes(List<Attribute> attrlist) {
        this.attributes = attrlist;
    }
}