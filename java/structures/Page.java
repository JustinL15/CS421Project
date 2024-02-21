import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Page {
    private List<Record> records;
    private Table template;

    public Page(Table template, List<Record> records) {
        this.records = records;
        this.template = template;
    }

    public Page(Table template, byte[] data) {
        this.records = new ArrayList<Record>();
        this.template = template;

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

    public List<Record> getRecords() {
        return records;
    }

    public Table getTemplate() {
        return template;
    }


}