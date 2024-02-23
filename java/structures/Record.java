import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<Object> values;
    private Table template;

    public Record(Table template, List<Object> values) {
        this.values = values;
        this.template = template;
    }

    public Record(Table template, ByteBuffer buffer) {
        this.template = template;
        this.values = new ArrayList<Object>();

        List<Attribute> attrs = template.getAttributes();
        byte[] nullbitmap = new byte[attrs.size()];
        for (int i = 0; i < attrs.size(); i++) {
            nullbitmap[i] = buffer.get();
        }

        for (int i = 0; i < attrs.size(); i++) {
            if (nullbitmap[i] == 1) {
                this.values.add(null);
            }
            switch (attrs.get(i).getDataType()) {
                case Integer:
                    this.values.add(buffer.getInt());
                    break;
                case Double:
                    this.values.add(buffer.getDouble());
                    break;
                case Boolean:
                    this.values.add(buffer.get() == 1);
                    break;
                case Char:
                    int clength = attrs.get(i).getMaxLength();
                    char[] charArr = new char[clength];

                    for (int j = 0; j < clength; j++) {
                        charArr[j] = buffer.getChar();
                    }
                    this.values.add(charArr);
                    break;
                case Varchar:
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

    public byte[] toByte() {
        int totalcap = this.values.size();
        List<Attribute> attrs = this.template.getAttributes();
        byte[] nullbitmap = new byte[this.values.size()];
        
        for (int i = 0; i < this.values.size(); i++) {
            if (this.values.get(i) == null) {
                nullbitmap[i] = 1;
                continue;
            }
            switch (attrs.get(i).getDataType()) {
                case Integer:
                    totalcap += 4;
                    break;
                case Double:
                    totalcap += 8;
                    break;
                case Boolean:
                    totalcap += 1;
                    break;
                case Char:
                    char[] cl = (char[]) this.values.get(i);
                    totalcap += (2 * cl.length);
                    break;
                case Varchar:
                    char[] vl = (char[]) this.values.get(i);
                    totalcap += (2 * vl.length) + 4;
                    break;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalcap);
        buffer.put(nullbitmap);
        
        for (int i = 0; i < this.values.size(); i++) {
            switch (attrs.get(i).getDataType()) {
                case Integer:
                    buffer.putInt((int) this.values.get(i));
                    break;
                case Double:
                    buffer.putDouble((double) this.values.get(i));
                    break;
                case Boolean:
                    if ((boolean) this.values.get(i)) {
                        buffer.put((byte) 1);
                    } else {
                        buffer.put((byte) 0);
                    }
                    break;
                case Char:
                    for (char c: (char[]) this.values.get(i)) {
                        buffer.putChar(c);
                    }
                    break;
                case Varchar:
                    char[] vchar = (char[]) this.values.get(i);
                    buffer.putInt(vchar.length);
                    for (char c: vchar) {
                        buffer.putChar(c);
                    }
                    break;
            }
        }
        return buffer.array();
    }

    public List<Object> getValues() {
        return this.values;
    }

    public Table getTemplate() {
        return this.template;
    }
    public void setTemplate(Table template){
        this.template = template;
    }

}