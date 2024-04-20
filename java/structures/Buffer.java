import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

// Queue of HardwarePages
// write is abstract
// read and readNode
// identify with table and number but verify type for each method
public class Buffer {
    private Queue<HardwarePage> pages;
    private Catalog catalog;
    private String databaseLocation;

    public Buffer(Catalog catalog, String databaseLocation) {
        this.pages = new LinkedList<HardwarePage>();
        this.catalog = catalog;
        this.databaseLocation = databaseLocation;
    }

    public Page read(String tableName, int pageNumber) {
        //System.out.println("Read");
        Table table = catalog.getTableByName(tableName);
        for (HardwarePage page : pages) {
            if (page.getPageNumber() == pageNumber && page.getTemplate() == table && page.getClass().equals(Page.class)) {
                pages.remove(page);
                pages.add(page);
                return (Page)page;
            }
        }

        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.separator + "tables" + File.separator + tableNumber;
        File tableFile = new File(tableLocation);
        RandomAccessFile tableAccessFile;
        try {
            tableAccessFile = new RandomAccessFile(tableFile, "r");
        } catch (FileNotFoundException e) {
            Page np = new Page(table, new ArrayList<Record>(), pageNumber);
            pages.add(np);
            return np;
        }

        int pageSize = catalog.getPageSize();
        byte[] bytes = new byte[pageSize];
        try {
            tableAccessFile.seek(pageSize * pageNumber);
            tableAccessFile.read(bytes, 0, pageSize); // different way to specify where in file (pagesize * pagenumber)
            tableAccessFile.close();
        } catch (IOException e) {
            System.out.println("IOException when reading from table " + tableNumber);
            return null;
        }

        Page page = new Page(table, bytes, pageNumber);
        pages.add(page);
        if (pages.size() > catalog.getBufferSize()) {
            Page lruPage = (Page)pages.remove();
            write(lruPage);
        }
        return page;
    }

    private void write(HardwarePage page) {
        if (page.bytesUsed() > catalog.getPageSize() && page.getClass().equals(Page.class)) {
            splitPage(page.getTemplate().getName(), (Page)page);
            return;
        }
        byte[] bytes = page.toByte(catalog.getPageSize());
        Table table = page.getTemplate();
        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.separator + "tables" + File.separator + tableNumber;
        File tableFile = new File(tableLocation);
        RandomAccessFile tableAccessFile;
        try {
            tableAccessFile = new RandomAccessFile(tableFile, "rw");
        } catch (FileNotFoundException e) {
            try {
                Path tablePath = Files.createFile(tableFile.toPath());
                tableAccessFile = new RandomAccessFile(tablePath.toString(), "rw");
            } catch (IOException io) {
                System.err.println(io);
                return;
            }
        }

        int pageSize = catalog.getPageSize();
        try {
            tableAccessFile.seek(page.getPageNumber() * pageSize);
            tableAccessFile.write(bytes, 0, bytes.length); // write at page.getPageNumber() * pageSize
            tableAccessFile.close();
        } catch (IOException e) {
            System.out.println("IOException when writing to table " + tableNumber);
            return;
        }
    }

    public void splitPage(String tableName, Page page) {
        Deque<Page> pagesToSplit = new ArrayDeque<>();
        pagesToSplit.add(page);
        int pageNumber = page.getPageNumber();
        while (pagesToSplit.peekFirst().bytesUsed() > catalog.getPageSize()) {
            int numPages = pagesToSplit.size();
            for (int i = 0; i < numPages; i++) {
                Page cur = pagesToSplit.poll();
                if (cur.bytesUsed() > catalog.getPageSize()) {
                    Page newPage = new Page(cur.getTemplate(), new ArrayList<>(), pageNumber);
                    for (int j = 0; j < cur.getRecords().size() / 2; j++) {
                        newPage.getRecords().add(0, cur.getRecords().remove(cur.getRecords().size() - 1));
                    }
                    pagesToSplit.add(cur);
                    pagesToSplit.add(newPage);
                } else {
                    pagesToSplit.add(cur);
                }
            }
        }

        Table table = catalog.getTableByName(tableName);
        List<Page> pagesToAdd = new ArrayList<>(pagesToSplit);
        for (int i = pageNumber + 1; i < table.getPagecount(); i++) {
            Page next = read(tableName, i);
            next.setPageNumber(i + pagesToAdd.size() - 1);
        }
        for (int i = 0; i < pagesToAdd.size(); i++) {
            pagesToAdd.get(i).setPageNumber(pageNumber + i);
            write(pagesToAdd.get(i));
            table.setPageCount(table.getPagecount() + pagesToAdd.size() - 1);
        }
    }

    public void purge() {
        for (HardwarePage page : pages){
            write(page);
        }
        pages = new LinkedList<HardwarePage>();
    }
    
    public void cleanTable(Table table) {
        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.separator + "tables" + File.separator + tableNumber;
        RandomAccessFile tableAccessFile;
        File tableFile = new File(tableLocation);
        try {
            tableAccessFile = new RandomAccessFile(tableFile, "rw");
            tableAccessFile.setLength(0);
        } catch (FileNotFoundException e) {
            System.out.println("Table does not exist.");
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) 
    {
        // Attribute a1 = new Attribute("name", Type.Varchar, 10, false, false, false);
        // Attribute a2 = new Attribute("number", Type.Integer, 0, false, false, false);
        // ArrayList<Attribute> as = new ArrayList<Attribute>();
        // as.set(0, a1);
        // as.set(1, a2);

        // Table table = new Table("test", 0, as,0);

        // ArrayList<Table> tables = new ArrayList<Table>();
        // tables.add(table);
        // Catalog cat = new Catalog(1, 20, tables);

        // Buffer buffer = new Buffer(cat, "Database-System-Implementation-Project\\resources");
        // Page page0 = buffer.read(table.getName(), 0);
        // List<Object> lst = new ArrayList<Object>();
        // lst.add("john".toCharArray());
        // lst.add(7);
        // page0.getRecords().add(new Record(table, lst));
        // buffer.purge();
        // Page page1 = buffer.read(table.getName(), 0);
        // char[] got = (char[]) page1.getRecords().get(0).getValues().get(0);
        // for (char c : got) {
        //     System.out.print(c);
        // }
        // System.out.println();
        // System.out.println((int) page1.getRecords().get(0).getValues().get(1));

        Queue<HardwarePage> pages = new LinkedList<>();
        // Table table = new Table("name", 0, null, 0);
        pages.add(new BPlusTreeNode(false));
        pages.add(new Page(null, new ArrayList<>(), 0));
        int i = 0;
        for (HardwarePage page : pages) {
            i++;
            System.out.println(i);
            if (page.getPageNumber() == 0 && page.getTemplate() == null && page.getClass().equals(Page.class)) {
                System.out.println("here");
            }
        }
    }
}