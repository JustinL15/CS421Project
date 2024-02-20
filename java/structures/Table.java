public class Table {
    private String name;
    private int number;
    private Attribute[] attributes;
    private int pagecount;
    private int recordcount;
    
    
    public Table(String name, int number, Attribute[] Attributes) {
        this.name = name;
        this.number = number;
        this.attributes = Attributes;
        this.pagecount = 1;
        this.recordcount = 0;

    }
    public String toString(){
        return null;
    }
    public Byte[] toBinary(){
        return null;
    };
    public Attribute[] getAttributes(){
        return null;
    }
    public String getName(){
        return name;
    }
    public int getNumber(){
        return number;
    }
    public int getPagecount(){
        return this.pagecount;
    }
    public int getRecordcount(){
        return this.recordcount;
    }
}