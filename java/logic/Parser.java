import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Attr;

public class Parser {
    StorageManager sM;
    public Parser(StorageManager StorageMan){
        sM = StorageMan;
    }

    public void create_table(String name, int number, String TableAttr){
        
        String[] Tablevals = TableAttr.split(",");
        Attribute[] AttrList = new Attribute[Tablevals.length];
        int k = 0;
        for(String i : Tablevals){
            String[] attribute_values = i.split(",");
            String ATTRname = attribute_values[0];
            String ATTRTYPE = attribute_values[1];
            boolean unquie = false;
            boolean nullable = true;
            boolean primkey = false;
            int length = 0;
            int decimal = 0;
            for( int j = 2; j <= attribute_values.length - 1; j++){
                if(attribute_values[j] == "UNQUIE"){
                    unquie = true;
                }
                if(attribute_values[j] == "NOT"){
                    if(j +1 <= attribute_values.length+1){
                        if(attribute_values[j] == "NULL"){
                            nullable = false;
                        }
                    }
                }
                if(attribute_values[j] == "PRIMARY"){
                    if(j +1 <= attribute_values.length+1){
                        if(attribute_values[j] == "KEY"){
                            primkey = true;
                        }
                    }
                }
            }
            Type attrtype1 = null;
            if(ATTRTYPE.substring(0, ATTRTYPE.indexOf("(")) == "VARCHAR"){
                length = Integer.parseInt( ATTRTYPE.substring(ATTRTYPE.indexOf("("),ATTRTYPE.indexOf(")")) );
                attrtype1 = Type.Varchar;
            }
            if(ATTRTYPE.substring(0, ATTRTYPE.indexOf("(")) == "CHAR"){
                length = Integer.parseInt( ATTRTYPE.substring(ATTRTYPE.indexOf("("),ATTRTYPE.indexOf(")")) );
                attrtype1 = Type.Char;
            }
            if(ATTRTYPE.substring(0, ATTRTYPE.indexOf("(")) == "DOUBLE"){
                String[] lengths = ATTRTYPE.substring(ATTRTYPE.indexOf("("),ATTRTYPE.indexOf(")")).split(",");
                length = Integer.parseInt(lengths[0]);
                decimal = Integer.parseInt(lengths[1]); 
                attrtype1 = Type.Double;
            }
            if(ATTRTYPE == "INT"){
                attrtype1 = Type.Integer;
            }
            if(ATTRTYPE == "BOOLEAN"){
                attrtype1 = Type.Boolean;
            }
            Attribute new_attr = new Attribute(ATTRname, attrtype1, length,decimal, nullable, primkey, unquie);
            AttrList[k] = new_attr;
            k = k +1;

        }
        sM.createTable(name,number,AttrList);

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