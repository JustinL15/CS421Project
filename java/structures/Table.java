import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Table {
    private String name;
    private int number;
    private List<Attribute> attributes;
    private int pagecount;
    private List<Integer> freeTreePages;
    private int nextTreePageNum;
    private int rootLocation;
    private int pkeyindex = -1;
    private List<Integer> pageorder; //index = order , value = pagenumber

    public Table(String name, int number, List<Attribute> attributes, int pagecount) {
        this.name = name;
        this.number = number;
        this.attributes = attributes;
        this.pagecount = pagecount;
        this.freeTreePages = new ArrayList<>();
        this.pageorder = new ArrayList<>();
        this.nextTreePageNum = 0;
        this.rootLocation = 0;
    }

    // creates a Table by deserializing a ByteBuffer
    public Table(ByteBuffer buffer) {
        this.name = "";
        int name_length = buffer.getInt();
        for (int i = 0; i < name_length; i++) {
            this.name += buffer.getChar();
        }
        this.number = buffer.getInt();
        this.pagecount = buffer.getInt();
        this.rootLocation = buffer.getInt();
        this.nextTreePageNum = buffer.getInt();
        this.freeTreePages = new ArrayList<>();
        int free_tree_page_length = buffer.getInt();
        for (int i = 0; i < free_tree_page_length; i++) {
            this.freeTreePages.add(buffer.getInt());
        }
        this.pageorder = new ArrayList<>();
        for (int i = 0; i < pagecount; i++) {
            this.pageorder.add(buffer.getInt());
        }
        int attributes_length = buffer.getInt();
        this.attributes = new ArrayList<Attribute>();
        for (int i = 0; i < attributes_length; i++) {
            this.attributes.add(new Attribute(buffer));
        }
    }

    public String toString() {
        String string = "";
        string += "\ttable name: " + this.name + "\n";
        string += "\ttable number: " + this.number + "\n";
        string += "\tattributes:\n";
        for (Attribute attribute : this.attributes) {
            string += attribute.toString() + "\n";
        }
        return string;
    }

    public int totalBytes(){
        int size = 0; // total bytes for binary representation
        size += Integer.BYTES + name.length() * 2; // name string w/ string length
        size += Integer.BYTES * 3; // ints number, length of attributes, and page count
        size += Integer.BYTES * (3 + freeTreePages.size() + pagecount);
        for (Attribute attribute : attributes) {
            size += attribute.totalBytes();
        }
        return size;
    }

    public void writeBytes(ByteBuffer buffer){
        buffer.putInt(name.length());
        for (char c : name.toCharArray()) {
            buffer.putChar(c);
        }
        buffer.putInt(number);
        buffer.putInt(pagecount);
        buffer.putInt(rootLocation);
        buffer.putInt(nextTreePageNum);
        buffer.putInt(freeTreePages.size());
        for (int freePageNum : freeTreePages) {
            buffer.putInt(freePageNum);
        }
        for (int pageordernum : pageorder) {
            buffer.putInt(pageordernum);
        }
        buffer.putInt(attributes.size());
        for (Attribute attribute : attributes) {
            attribute.writeBytes(buffer);
        }
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public String getName() {
        return name;
    }

    public int getNumber() {
        return number;
    }
    public void setNumber(int number) {
        this.number = number;
    }
    public int getPagecount(){
        return this.pagecount;
    }
    public List<Integer> getPageOrder(){
        return this.pageorder;
    }

    public void setPageOrder(List<Integer> PageOrder) {
        this.pageorder = PageOrder;
    }

    public void setPageCount(int number){
        this.pagecount = number;
    }

    public void setAttributes(List<Attribute> attrlist) {
        this.attributes = attrlist;
    }

    public int getNextFreePage() {
        if (freeTreePages.size() != 0) {
            return freeTreePages.remove(0);
        } else {
            nextTreePageNum += 1;
            return nextTreePageNum - 1;
        }
    }
    
    public int getNextPageCount() {
        int next = getPagecount();
        setPageCount(next + 1);
        return next;
    }

    public void addFreePage(int pageNum) {
        freeTreePages.add(pageNum);
    }

    public int getRootPage() {
        return rootLocation;
    }

    public void setRootPage(int pageNum) {
        rootLocation = pageNum;
    }

    public int getPrimaryKeyIndex() {
        if (pkeyindex != -1) {
            return pkeyindex;
        }
        List<Attribute> attr = this.getAttributes();
        for (int i = 0; i < attr.size(); i++) {
            if (attr.get(i).isKey()) {
                pkeyindex = i;
                return i;
            }
        }
        return -1;
    }

    public int getMaxPKeySize() {
        Attribute pKey = this.attributes.get(getPrimaryKeyIndex());
        switch(pKey.getDataType()) {
            case Integer:
                return Integer.BYTES;
            case Double:
                return Double.BYTES;
            case Boolean:
                return 1;
            case Char:
            case Varchar:
                return (pKey.getMaxLength() * Character.BYTES) + Integer.BYTES;
            default:
                return -1;
        }
    }

    public Object transformStringPrimaryKey(String str) {
        switch(getAttributes().get(getPrimaryKeyIndex()).getDataType()) {
            case Integer:
                return Integer.parseInt(str);
            case Double:
                return Double.parseDouble(str);
            case Boolean:
                return (str.toLowerCase() == "true");
            case Char:
            case Varchar:
                return str;
        }
        return null;
    }
}