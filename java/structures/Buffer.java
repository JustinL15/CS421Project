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

public class Buffer {
    private Queue<HardwarePage> pages;
    private Catalog catalog;
    private String databaseLocation;

    public Buffer(Catalog catalog, String databaseLocation) {
        this.pages = new LinkedList<HardwarePage>();
        this.catalog = catalog;
        this.databaseLocation = databaseLocation;
    }

    public HardwarePage read(String tableName, int pageNumber, boolean readingNode) {
        //System.out.println("Read");
        Object pageClass = readingNode ? BPlusTreeNode.class : Page.class;
        Table table = catalog.getTableByName(tableName);
        for (HardwarePage page : pages) {
            if (page.getPageNumber() == pageNumber && page.getTemplate() == table && page.getClass().equals(pageClass)) {
                pages.remove(page);
                pages.add(page);
                return page;
            }
        }

        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.separator + (readingNode ? "trees" : "tables") + File.separator + tableNumber;
        File tableFile = new File(tableLocation);
        RandomAccessFile tableAccessFile;
        try {
            tableAccessFile = new RandomAccessFile(tableFile, "r");
        } catch (FileNotFoundException e) {
            if (!readingNode) {
                Page np = new Page(table, new ArrayList<Record>(), pageNumber);
                addPage(np);
                return np;
            }
            else {
                BPlusTreeNode newNode = new BPlusTreeNode(true, (catalog.getPageSize()/(table.getMaxPKeySize() + 8)), table);
                addPage(newNode);
                return newNode;
            }
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

        HardwarePage page;
        if (!readingNode) {
            page = new Page(table, bytes, pageNumber);
        }
        else {
            page = new BPlusTreeNode(bytes, (catalog.getPageSize()/(table.getMaxPKeySize() + 8)), table, pageNumber);
            page = null;
        }
        addPage(page);
        return page;
    }

    public void addPage(HardwarePage page) {
        pages.add(page);
        if (pages.size() > catalog.getBufferSize()) {
            HardwarePage lruPage = pages.remove();
            write(lruPage);
        }
    }

    public void write(HardwarePage page) {
        if (page.bytesUsed() > catalog.getPageSize() && page.getClass().equals(Page.class)) {
            splitPage(page.getTemplate().getName(), (Page)page);
            return;
        }
        byte[] bytes = page.toByte(catalog.getPageSize());
        Table table = page.getTemplate();
        int tableNumber = table.getNumber();
        String tableLocation = databaseLocation + File.separator + (page.getClass().equals(Page.class) ? "tables" : "trees") + File.separator + tableNumber;
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
            System.out.println("Page number: " + page.getPageNumber());
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
        Table table = catalog.getTableByName(tableName);
        int pageNumber = page.getPageNumber();

        while (pagesToSplit.peekFirst().bytesUsed() > catalog.getPageSize()) {
            int numPages = pagesToSplit.size();
            for (int i = 0; i < numPages; i++) {
                Page cur = pagesToSplit.poll();
                if (cur.bytesUsed() > catalog.getPageSize()) {
                    Page newPage = new Page(cur.getTemplate(), new ArrayList<>(), table.getNextFreePage());
                    for (int j = 0; j < cur.getRecords().size() / 2; j++) {
                        newPage.getRecords().add(0, cur.getRecords().remove(cur.getRecords().size() - 1));
                    }
                    
                    if (catalog.getBPlusIndex()) {
                        BPlusTreeNode root = (BPlusTreeNode) read(tableName, table.getRootPage() ,true);
                        try{
                        for (int j = 0; j < newPage.getRecords().size(); j++) {
                            root.updateNodePointer(newPage.getRecords().get(j).getPrimaryKey(),new int[]{newPage.getPageNumber(),j}, this);
                        }
                        }catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }

                    pagesToSplit.add(cur);
                    pagesToSplit.add(newPage);
                } else {
                    pagesToSplit.add(cur);
                }
            }
        }

        List<Page> pagesToAdd = new ArrayList<>(pagesToSplit);
        List<Integer> x = table.getPageOrder();
        
        int pageordernum = -1;
        for (int i = 0; i < x.size(); i++){
            if(x.get(i) == pageNumber){
                pageordernum = i;
                break;
            }
        }
        
        for (int i = 1; i < pagesToAdd.size(); i++) {
            x.add(pageordernum + i, pagesToAdd.get(i).getPageNumber());
        }

        ///
        /// for all pages in pages to add assign them a page number at the end of the page list.
        //make a list in table that keeps track of order.   
        //jst insert the page number into index of split page plus 1 + index of page in list?
    }

    public void purge() {
        // for (HardwarePage page : pages){
        //     write(page);
        // }
        while (!pages.isEmpty()) {
            write(pages.remove());
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
        // pages.add(new BPlusTreeNode(false));
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