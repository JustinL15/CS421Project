import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser {
    StorageManager sM = new StorageManager();

    public void create_table(String name, int number, Attribute[] TableAttr){
        
        Table New_Table = new Table(name,number,TableAttr);
        sM.createTable(name,number,TableAttr);

    }
    public void drop_table(String name){
        sM.dropTable(name);   
    }
    public void alter_table(){
        
    }
    public void insert_values(Table table, String[] order, String[] values){
        Map<String, Integer> columnOrder = new HashMap<>();
        Attribute[] tableCol = table.getAttributes();
        List<Object> cVals = new ArrayList<Object>();


        for (int i = 0; i < tableCol.length; i++) {
            columnOrder.put(tableCol[i].getName(), i);
            cVals.add(null);
        }

        for (int i = 0; i < order.length; i++) {
            Integer orderNum = columnOrder.get(order[i]);
            if (orderNum == null) {
                // return an error
                return;
            }
            switch (tableCol[i].getDataType()) {
                case Integer:
                    cVals.set(orderNum, Integer.parseInt(values[i]));
                    break;
                case Double:
                    cVals.set(orderNum, Double.parseDouble(values[i]));
                    break;
                case Boolean:
                    cVals.set(orderNum, Boolean.parseBoolean(values[i]));
                    break;
                case Char:
                case Varchar:
                    cVals.set(orderNum, values[i].toCharArray());
                    break;
            }
        }
        Record record = new Record(table, cVals);
        ArrayList<Record> records = new ArrayList<>();
        records.add(record);
        
        // convert data into this function
        sM.insertRecord_table(table, records);
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