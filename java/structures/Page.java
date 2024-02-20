import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Page {
    private ByteBuffer data;
    private List<Record> records;
    private int frontPtr;
    private int freePtr;
    private List<Integer[]> freeArray = new ArrayList<>();
    private Table template;

    public Page(int max_size, Table template) {
        this.template = template;
        this.records = new ArrayList<>();
        this.freePtr = max_size - 1;
        this.frontPtr = 4;
        this.data = ByteBuffer.allocate(max_size);
        data.putInt(0);
    }
}