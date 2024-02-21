import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Queue;

public class Buffer {
    private Queue<Page> pages;
    private Catalog catalog;
    private String databaseLocation;

    public Buffer(Catalog catalog, String databaseLocation){
        this.pages = new LinkedList<Page>();
        this.catalog = catalog;
        this.databaseLocation = databaseLocation;
    }
    public Page read(String tableName, int pageNumber){
        int tableNumber = this.catalog.getTableByName(tableName).getNumber();
        String tableLocation = this.databaseLocation;
        return null;
    }
    private void write(){}
    private void purge(){}
}