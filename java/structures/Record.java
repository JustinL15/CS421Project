import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<Object> values;
    private byte[] data;

    public Record(Table template, byte[] data) {
        this.values = new ArrayList<Object>();
        this.data = data;

        ByteBuffer buffer = ByteBuffer.wrap(data);
        Attribute[] attrs = template.getAttributes();
        byte[] nullbitmap = new byte[attrs.length];
        for (int i = 0; i < attrs.length; i++) {
            nullbitmap[i] = buffer.get();
        }

        for (int i = 0; i < attrs.length; i++) {
            if (nullbitmap[i] == 1) {
                continue;
            }
            switch (attrs[i].getDataType()) {
                case "Integer":
                    this.values.add(buffer.getInt());
                    break;
                case "Double":
                    this.values.add(buffer.getDouble());
                    break;
                case "Boolean":
                    this.values.add(buffer.get() == 1);
                    break;
                case "Char":
                    int clength = attrs[i].getMaxLength();
                    char[] charArr = new char[clength];

                    for (int j = 0; j < clength; j++) {
                        charArr[j] = buffer.getChar();
                    }
                    this.values.add(charArr);
                    break;
                case "Varchar":
                    int vlength = buffer.getInt();
                    char[] vcharArr = new char[vlength];

                    for (int j = 0; j < vlength; j++) {
                        vcharArr[j] = buffer.getChar();
                    }
                    this.values.add(vcharArr);
                    break;
            }
        }
    }

    public Record(Table template, List<Object> values) {
        int totalcap = values.size();
        Attribute[] attrs = template.getAttributes();
        byte[] nullbitmap = new byte[values.size()];
        
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null) {
                nullbitmap[i] = 1;
                continue;
            }
            switch (attrs[i].getDataType()) {
                case "Integer":
                    totalcap += 4;
                    break;
                case "Double":
                    totalcap += 8;
                    break;
                case "Boolean":
                    totalcap += 1;
                    break;
                case "Char":
                    char[] cl = (char[]) values.get(i);
                    totalcap += (2 * cl.length);
                    break;
                case "Varchar":
                    char[] vl = (char[]) values.get(i);
                    totalcap += (2 * vl.length);
                    break;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalcap);
        buffer.put(nullbitmap);
        
        for (int i = 0; i < values.size(); i++) {
            switch (attrs[i].getDataType()) {
                case "Integer":
                    buffer.putInt((int) values.get(i));
                    break;
                case "Double":
                    buffer.putDouble((double) values.get(i));
                    break;
                case "Boolean":
                    if ((boolean) values.get(i)) {
                        buffer.put((byte) 1);
                    } else {
                        buffer.put((byte) 0);
                    }
                    break;
                case "Char":
                    for (char c: (char[]) values.get(i)) {
                        buffer.putChar(c);
                    }
                    break;
                case "Varchar":
                    char[] vchar = (char[]) values.get(i);
                    buffer.putInt(vchar.length);
                    for (char c: vchar) {
                        buffer.putChar(c);
                    }
                    break;
            }
        }
    }

    public List<Object> getValues() {
        return this.values;
    }

    public byte[] getData() {
        return this.data;
    }
}