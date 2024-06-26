import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Page implements HardwarePage {
    private List<Record> records;
    private Table template;
    private int pageNumber;

    public Page(Table template, List<Record> records, int pageNumber) {
        this.records = records;
        this.template = template;
        this.pageNumber = pageNumber;
    }

    public Page(Table template, byte[] data, int pageNumber) {
        this.records = new ArrayList<Record>();
        this.template = template;
        this.pageNumber = pageNumber;

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int numRecords = buffer.getInt();

        for (int i = 0; i < numRecords; i++) {
            records.add(new Record(template, buffer));
        }
    }

    public byte[] toByte(int max_size) {
        ByteBuffer buffer =  ByteBuffer.allocate(max_size);
        buffer.putInt(records.size());
        for (Record record: records) {
            buffer.put(record.toByte());
        }
        return buffer.array();
    }

    public int bytesUsed() {
        int used = 4;
        // for (Record record: records) {
        //     used += record.spacedUsed();
        // }
        if (records.size() == 0) {
            return used;
        }
        used += records.get(0).maxSpaceUsed() * records.size();
        return used;
    }

    public List<Record> getRecords() {
        return records;
    }

    public Table getTemplate() {
        return template;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int number) {
        this.pageNumber = number;
    }

    public static void main(String[] args) 
    {
        ArrayList<Attribute> attr = new ArrayList<Attribute>();
        attr.set(0, new Attribute("yes/no", Type.Integer, 0, false, false, false));
        attr.set(1, new Attribute("name", Type.Varchar, 3, false, false, false));
        Table table = new Table("test", 0, attr,0);
        List<Object> vals = new ArrayList<Object>();
        vals.add(1);
        char[] c = new char[] {'a', 'b', 'c'};
        vals.add(c);
        Record record1 = new Record(table, vals);
        System.out.println(record1.getValues());
        for (char ch: (char[]) record1.getValues().get(1)) {
            System.out.println(ch);
        }
        System.out.println(record1.toByte());
        Record record1t = new Record(table, ByteBuffer.wrap(record1.toByte()));
        System.out.println(record1t.getValues());   
        for (char ch: (char[]) record1t.getValues().get(1)) {
            System.out.println(ch);
        }
        List<Record> records = new ArrayList<Record>();
        records.add(record1);
        records.add(record1t);
        Page np = new Page(table, records, 0);
        byte[] data1 = np.toByte(50);
        Page npt = new Page(table, data1, 1);
        byte[] data2 = npt.toByte(50);
        for (byte b: data1) {
            System.out.print(b);
        }
        System.out.println();
        for (byte b: data2) {
            System.out.print(b);
        }
        
    }

}