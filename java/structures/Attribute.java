import java.nio.ByteBuffer;

public class Attribute {
    private String name;
    private Type data_type;
    private int max_length;
    private boolean not_null;
    private boolean key;
    private boolean unique;

    public Attribute(String name, Type data_type, int max_length, boolean not_null, boolean key, boolean unique) {
        this.name = name;
        this.data_type = data_type;
        this.max_length = max_length;
        this.not_null = not_null;
        this.key = key;
        this.unique = unique;
    }

    // creates an Attribute by deserializing a ByteBuffer
    public Attribute(ByteBuffer buffer) {
        this.data_type = Type.values()[buffer.getInt()];
        this.max_length = buffer.getInt();
        this.not_null = buffer.get() == 1;
        this.key = buffer.get() == 1;
        this.unique = buffer.get() == 1;
        this.name = "";
        int name_length = buffer.getInt();
        for (int i = 0; i < name_length; i++) {
            this.name += buffer.getChar();
        }
    }

    public String toString() {
        String string = "";
        string += "\t\tattribute name: " + this.name + "\n";
        string += "\t\tdata type: " + this.data_type.name() + "\n";
        if (this.data_type == Type.Char || this.data_type == Type.Varchar) {
            string += "\t\tmax length: " + this.max_length + "\n";
        }
        string += "\t\tnot null: " + this.not_null + "\n";
        string += "\t\tkey: " + this.key + "\n";
        string += "\t\tunique: " + this.unique + "\n";
        return string;
    }

    public int totalBytes() {
        int size = 0; // total bytes for binary representation
        size += Integer.BYTES + name.length() * 2; // name string w/ string length
        size += Integer.BYTES * 2; // ints data_type and max_length
        size += 3; // booleans nullable, key, and unique
        return size;
    }

    public void writeBytes(ByteBuffer buffer) {
        buffer.putInt(data_type.ordinal());
        buffer.putInt(max_length);
        buffer.put(not_null ? (byte) 1 : (byte) 0);
        buffer.put(key ? (byte) 1 : (byte) 0);
        buffer.put(unique ? (byte) 1 : (byte) 0);
        buffer.putInt(name.length());
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

    public boolean isKey() {
        return this.key;
    }

    public boolean isUnique() {
        return this.unique;
    }

    public boolean isNotNull() {
        return this.not_null;
    }
    public int getbytesize(Object val){
        if(data_type == Type.Boolean){
            return 1;
        }
        else if(data_type == Type.Char){
            char[] cl = (char[]) val;
            return (2 * cl.length);
        }
        else if(data_type == Type.Varchar){
            char[] vl = (char[]) val;
            return (2 * vl.length) + 4;
        }
        else if(data_type == Type.Integer){
            return 4;
        }
        else if(data_type == Type.Double){
            return 8;
        }
        else{
            return 0;
        }
    }
    public void set_name(String new_name){
        this.name = new_name;
    }
}