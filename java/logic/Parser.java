import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class Parser {
    StorageManager sM;
    public Parser(StorageManager StorageMan){
        sM = StorageMan;
    }

    public void create_table(String name, int number, String TableAttr){
        
        String[] Tablevals = TableAttr.split(",");
        ArrayList<Attribute> AttrList = new ArrayList<Attribute>();
        int k = 0;
        Boolean primaryKeyPresent = false;
        for(String i : Tablevals){
            String[] attribute_values = i.split(",");
            String ATTRname = attribute_values[0];
            String ATTRTYPE = attribute_values[1];
            boolean unquie = false;
            boolean nullable = true;
            boolean primkey = false;
            int length = 0;
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
                            if (primaryKeyPresent == true) {
                                System.out.println("More than one primary key");
                                return;
                            } else {
                                primaryKeyPresent = true;
                            }
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
            if(ATTRTYPE == "DOUBLE"){
                
                attrtype1 = Type.Double;
            }
            if(ATTRTYPE == "INT"){
                attrtype1 = Type.Integer;
            }
            if(ATTRTYPE == "BOOLEAN"){
                attrtype1 = Type.Boolean;
            }
            Attribute new_attr = new Attribute(ATTRname, attrtype1, length, nullable, primkey, unquie);
            AttrList.set(k, new_attr);
            k = k +1;

        }
        if (primaryKeyPresent == false){
            System.out.println("No primary key defined");
            return;
        }
        sM.createTable(name,number,AttrList);

    }
    public void drop_table(String name){
        sM.dropTable(name);   
    }
    public void add_table_column(Table table, Attribute newAttr,String defaultval){
        
        sM.add_table_column(table, newAttr,defaultval);
        
    }
    public void delete_table_column(Table table, String deleteAttribute){
        
        sM.delete_table_column(table, deleteAttribute);
        
    }
    public void insert_values(String tableName, List<String> values) throws Exception {
        Table table = sM.catalog.getTableByName(tableName);
        List<Attribute> tableCol = table.getAttributes();
        List<Object> cVals = new ArrayList<Object>();
        
        for (int i = 0; i < tableCol.size(); i++) {
            try {
                String val = values.get(i);
                if (val.equals("null")) {
                    if (tableCol.get(i).isNotNull()) {
                        throw new Exception("Attribute '" + tableCol.get(i).getName() +"' cannot be null.");
                    } else {
                        cVals.add(null);
                    }
                } else {
                    switch (tableCol.get(i).getDataType()) {
                        case Integer:
                            cVals.add(Integer.parseInt(val));
                            break;
                        case Double:
                            cVals.add(Double.parseDouble(val));
                            break;
                        case Boolean:
                            cVals.add(Boolean.parseBoolean(val));
                            break;
                        case Char:
                        case Varchar:
                            cVals.add(val.toCharArray());
                            break;
                    }
                }
            } catch (IndexOutOfBoundsException e) {
                    if (tableCol.get(i).isNotNull()) {
                        throw new Exception("Attribute '" + tableCol.get(i).getName() +"' cannot be null.");
                    } else {
                        cVals.add(null);
                    }
            }
        }
        for (int i = 0; i < cVals.size(); i++) {
            if (tableCol.get(i).isUnique()) {
                switch (tableCol.get(i).getDataType()) {
                    case Integer:
                            if (sM.checkUnique(table, i, (int) cVals.get(i))) {
                                throw new Exception("Attribute '" + tableCol.get(i).getName() +"' has to be unique.");
                            }
                            break;
                        case Double:
                            if (sM.checkUnique(table, i, (double) cVals.get(i))) {
                                throw new Exception("Attribute '" + tableCol.get(i).getName() +"' has to be unique.");
                            }
                            break;
                        case Boolean:
                            if (sM.checkUnique(table, i, (boolean) cVals.get(i))) {
                                throw new Exception("Attribute '" + tableCol.get(i).getName() +"' has to be unique.");
                            }
                            break;
                        case Char:
                        case Varchar:
                            if (sM.checkUnique(table, i, (char[]) cVals.get(i))) {
                                throw new Exception("Attribute '" + tableCol.get(i).getName() +"' has to be unique.");
                            }
                            break;
                }
            }
        }

        Record record = new Record(table, cVals);
        ArrayList<Record> records = new ArrayList<>();
        records.add(record);
        
        // convert data into this function
        sM.insertRecord_table(table, records);
    }


    public void print_display_schema(Catalog curr_cat,String file_loc){
        System.out.println("database location: "+ file_loc);
        System.out.println("page size: "+ curr_cat.getPageSize());
        System.out.println("buffer size: "+curr_cat.getBufferSize());
        for(Table i : curr_cat.getTables()){
            table_schema_display(i);
        }
        if(curr_cat.getTables().size() == 0){
            System.out.println("No tables to display");
        }
        
    }

    public void print_display_info(Table table){
      
        System.out.println("table name: "+ table.getName());
        System.out.println("table schema: "+ table.toString());    
        System.out.println("number of pages: " + table.getPagecount());
        System.out.println("number of records: " + table.getRecordcount());
        table_schema_display(table);

    }
    private void table_schema_display(Table table){
        
        System.out.println("Table name:"+table.getName());
        System.out.println("Table schema");
        for(Attribute i : table.getAttributes()){
            System.out.print("    "+i.getName()+":"+i.getDataType());
            if(i.getDataType() == Type.Varchar || i.getDataType() == Type.Char){
                System.out.print("("+i.getMaxLength()+")");
            }
            if(i.isKey()){
                System.out.print(" primarykey");
            }
            System.out.println("\n");

        }
        System.out.println("Pages:"+table.getPagecount());
        System.out.println("Records:"+table.getRecordcount());

        
        
    }
    public void select_statment(Table table){
        
        sM.getRecords_tablenumber(table.getNumber());
        //prints strings
    }

}