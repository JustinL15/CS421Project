import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Queue;

public class Buffer {
    private Queue<Page> pages;
    private Catalog catalog;
    private String databaseLocation;

    public Buffer(Catalog catalog, String databaseLocation) {
        this.pages = new LinkedList<Page>();
        this.catalog = catalog;
        this.databaseLocation = databaseLocation;
    }

    public Page read(String tableName, int pageNumber) {
        Table table = catalog.getTableByName(tableName);
        for (Page page : pages) {
            if (page.getPageNumber() == pageNumber) {
                pages.remove(page);
                pages.add(page);
                return page;
            }
        }

        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.pathSeparator + tableNumber;
        File tableFile = new File(tableLocation);
        RandomAccessFile tableAccessFile;
        try {
            tableAccessFile = new RandomAccessFile(tableFile, "r");
        } catch (FileNotFoundException e) {
            System.out.println("No file at " + tableLocation);
            return null;
        }

        int pageSize = catalog.getPageSize();
        byte[] bytes = new byte[pageSize];
        try {
            tableAccessFile.read(bytes, pageNumber * pageSize, pageSize);
            tableAccessFile.close();
        } catch (IOException e) {
            System.out.println("IOException when reading from table " + tableNumber);
            return null;
        }

        Page page = new Page(table, bytes, pageNumber);
        pages.add(page);
        if (pages.size() > catalog.getBufferSize()) {
            Page lruPage = pages.remove();
            write(lruPage);
        }
        return page;
    }

    private void write(Page page) {
        byte[] bytes = page.toByte(catalog.getPageSize());
        Table table = page.getTemplate();
        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.pathSeparator + tableNumber;
        File tableFile = new File(tableLocation);
        RandomAccessFile tableAccessFile;
        try {
            tableAccessFile = new RandomAccessFile(tableFile, "rw");
        } catch (FileNotFoundException e) {
            System.out.println("No file at " + tableLocation);
            return;
        }

        int pageSize = catalog.getPageSize();
        try {
            tableAccessFile.write(bytes, page.getPageNumber() * pageSize, pageSize);
            tableAccessFile.close();
        } catch (IOException e) {
            System.out.println("IOException when writing to table " + tableNumber);
            return;
        }
    }

    public void purge() {
        for (Page page : pages){
            write(page);
        }
        pages = new LinkedList<Page>();
    }
}