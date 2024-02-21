import java.util.HashMap;
import java.util.Map;

public class Parser {
    StorageManager sM = new StorageManager();

    public void create_table(String name, int number, Attribute[] TableAttr){
        
        Table New_Table = new Table(name,number,TableAttr);
        sM.createTable(New_Table);

    }
    public void drop_table(String name){
        sM.dropTable(name);   
    }
    public void alter_table(){
        
    }
    public void insert_values(Table table, String[] order, String[] values){
        Map<String, Integer> columnOrder = new HashMap<>();
        Attribute[] tableCol = table.getAttributes();
        String[] correctedVals = new String[values.length];

        for (int i = 0; i < tableCol.length; i++) {
            columnOrder.put(tableCol[i].getName(), i);
        }
        for (int i = 0; i < order.length; i++) {
            Integer orderNum = columnOrder.get(order[i]);
            if (orderNum == null) {
                // return an error
                return;
            }
            correctedVals[orderNum] = values[i];
        }
        // convert data into this function
        sM.insertRecord_table();
    }
    public void print_display_schema(){

    }

    public void print_display_info(Table table){
      
        System.out.println("table name: "+ table.getName());
        System.out.println("table schema: "+ table.toString());    
        System.out.println("number of pages: " + table.getPagecount());
        System.out.println("number of records: " + table.getRecordcount());
        

    }
    public void select_statment(Table table){
        
        sM.getRecords_tablenumber(table.getNumber());
        //prints strings
    }

}