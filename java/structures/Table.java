public class Table {
    private String name;
    private int number;
    private Attribute[] attributes;
    
    
    public Table(String name, int number, Attribute[] Attributes) {
        this.name = name;
        this.number = number;
        this.attributes = Attributes;

    }
    public String toString(){
        return null;
    }
    public Byte[] toBinary(){
        return null;
    }
    public Attribute[] getAttributes(){
        return null;
    }
    public String getName(){
        return name;
    }
    public int getNumber(){
        return number;
    }
}