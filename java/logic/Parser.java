import java.util.ArrayList;
import java.util.List;



public class Parser {
    StorageManager sM;
    public Parser(StorageManager StorageMan){
        sM = StorageMan;
    }

    public void create_table(String name, int number, String TableAttr){
        for (Table table : sM.catalog.getTables()) {
            if (name.compareTo(table.getName()) == 0){
                System.out.println("Table of name " + name + " already exists");
                return;
            }
        }
        String[] Tablevals = TableAttr.split(",");
        ArrayList<Attribute> AttrList = new ArrayList<Attribute>();
        Boolean primaryKeyPresent = false;
        for(String i : Tablevals){
            String[] attribute_values = i.split(" ");
            System.out.println(attribute_values[0] + " " + attribute_values[1]);
            String ATTRname = attribute_values[0];
            String ATTRTYPE = attribute_values[1];
            boolean unique = false;
            boolean nullable = true;
            boolean primkey = false;
            int length = 0;
            for( int j = 2; j <= attribute_values.length - 1; j++){
                if(attribute_values[j].compareTo("UNIQUE") == 0){
                    unique = true;
                }
                if(attribute_values[j].compareTo("NOT") == 0) {
                    if(j +1 <= attribute_values.length-1){
                        if(attribute_values[j+1].compareTo("NULL") == 0) {
                            nullable = false;
                        }
                    }
                }
                if(attribute_values[j].compareTo("PRIMARY") == 0) {
                    if(j +1 <= attribute_values.length-1){
                        if(attribute_values[j+1].compareTo("KEY") == 0) {
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
            if(ATTRTYPE.contains("(") && ATTRTYPE.substring(0, ATTRTYPE.indexOf("(")).compareTo("VARCHAR") == 0){
                length = Integer.parseInt( ATTRTYPE.substring(ATTRTYPE.indexOf("(")+1,ATTRTYPE.indexOf(")")) );
                attrtype1 = Type.Varchar;
            }
            else if(ATTRTYPE.contains("(") && ATTRTYPE.substring(0, ATTRTYPE.indexOf("(")).compareTo("CHAR") == 0){
                length = Integer.parseInt( ATTRTYPE.substring(ATTRTYPE.indexOf("(")+1,ATTRTYPE.indexOf(")")) );
                attrtype1 = Type.Char;
            }
            else if(ATTRTYPE.compareTo("DOUBLE") == 0){
                
                attrtype1 = Type.Double;
            }
            else if(ATTRTYPE.compareTo("INT") == 0){
                attrtype1 = Type.Integer;
            }
            else if(ATTRTYPE.compareTo("BOOLEAN") == 0){
                attrtype1 = Type.Boolean;
            }
            else {
                System.out.println("Invalid data type \"" + ATTRTYPE + "\"");
                return;
            }
            Attribute new_attr = new Attribute(ATTRname, attrtype1, length, nullable, primkey, unique);
            AttrList.add(new_attr);

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
    public void add_table_column(Table table, Attribute newAttr,String defaulttoken){
        
        Object defaultval = null; 
        try{
            switch (newAttr.getDataType()) {
                case Integer:
                    defaultval = Integer.parseInt(defaulttoken);
                    break;
                case Double:
                    defaultval = Double.parseDouble(defaulttoken);
                    break;
                case Boolean:
                    if (defaulttoken.compareTo("true") == 0) {
                        defaultval = true;
                    }
                    else if(defaulttoken.compareTo("false") == 0) {
                        defaultval = true;
                    }
                    else{
                        System.out.println("Error incorrect value for Attribute type");
                        return;
                    }
                    break;
                case Char:
                    char[] defaultchar = new char[defaulttoken.length()];
 
                    for (int i = 0; i < defaulttoken.length(); i++) {
                        defaultchar[i] = defaulttoken.charAt(i);
                    }
                    defaultval = defaultchar;
                    break;
                case Varchar:
                    char[] defaultvchar = new char[defaulttoken.length()];
 
                    for (int i = 0; i < defaulttoken.length(); i++) {
                        defaultvchar[i] = defaulttoken.charAt(i);
                     }
                    defaultval = defaultvchar;
                    break;
            }
        }
        catch (NumberFormatException e){
            System.out.println("Error incorrect value for Attribute type");
            return;
        }
        List<Attribute> attrlist =  table.getAttributes();
        for(Attribute i : attrlist){
            if(i.getName() == newAttr.getName()){
                System.out.println("Attribute name already in use");
                System.out.println(newAttr.getName()+" != "+defaulttoken);
                return;
            }
        }
        sM.add_table_column(table, newAttr,defaultval);
        
    }
    public void delete_table_column(Table table, String deleteAttribute){
        List<Attribute> attrlist =  table.getAttributes();
        //Attribute deleteAttrval = null;
        int found = -1;

        for (int i = 0; i < attrlist.size(); i++) {
            if(attrlist.get(i).getName().compareTo(deleteAttribute) == 0){
                //deleteAttrval = attrlist.get(i);
                attrlist.remove(i);
                //exit loop
            }
        }
        if(found == -1){
            System.out.println("Attribute was not found in table");
            return;
        }
        sM.delete_table_column(table, deleteAttribute, found);
        
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