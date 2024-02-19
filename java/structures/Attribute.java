import java.nio.ByteBuffer;

public class Attribute {
    private String name;
    private Type data_type;
    private int max_length;
    private boolean nullable;
    private boolean key;
    private boolean unique;
    
    public Attribute(String name, Type data_type, int max_length, boolean nullable, boolean key, boolean unique){
        this.name = name;
        this.data_type = data_type;
        this.max_length = max_length;
        this.nullable = nullable;
        this.key = key;
        this.unique = unique;
    }
    public byte[] toBinary(){
        int name_length = name.length() * 2; // number of bytes occupied by name
        int size = name_length + 15; // total bytes for binary representation (3 ints (4 bytes) and 3 bools (1 byte))
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(name_length);
        for (char c: name.toCharArray()) {
            buffer.putChar(c);
        }
        buffer.putInt(data_type.ordinal());
        buffer.putInt(max_length);
        buffer.put( nullable ? (byte)1 : (byte)0 );
        buffer.put( key ? (byte)1 : (byte)0 );
        buffer.put( unique ? (byte)1 : (byte)0 );
        
        return buffer.array();
    }
    public String getName(){
        return this.name;
    }
    public Type getDataType(){
        return this.data_type;
    }
    public int getMaxLength(){
        return this.max_length;
    }
}