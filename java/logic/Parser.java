import java.util.HashMap;
import java.util.Map;

public class Parser {
    StorageManager sM = new StorageManager();

    public void create_table(){

    }
    public void drop_table(){
        
    }
    public void alter_table(){
        
    }
    public void insert_values(Table table, String[] order, String[] values){
        Map<String, Integer> dictionary = new HashMap<>();
        for (Attribute attr: table.getAttributes()) {

        }
        for (String spec: order) {

        }
    }
    public void print_display_schema(){

    }
    public void print_display_info(){
        
    }
    //select state
    public Table get_table(){
        return null;
    }
}