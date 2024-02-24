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
        String[] Tablevals = TableAttr.split(",", -1);
        ArrayList<Attribute> AttrList = new ArrayList<Attribute>();
        Boolean primaryKeyPresent = false;
        for(String i : Tablevals){
            String[] attribute_values = i.trim().split(" ", -1);
            String ATTRname = attribute_values[0];
            String ATTRTYPE = attribute_values[1];
            boolean unique = false;
            boolean nullable = true;
            boolean primkey = false;
            int length = 0;
            for( int j = 2; j <= attribute_values.length - 1; j++){
                if(attribute_values[j].toLowerCase().compareTo("unique") == 0){
                    unique = true;
                }
                if(attribute_values[j].toLowerCase().compareTo("not") == 0) {
                    if(j +1 <= attribute_values.length-1){
                        if(attribute_values[j+1].toLowerCase().compareTo("null") == 0) {
                            nullable = false;
                        }
                    }
                }
                if(attribute_values[j].toLowerCase().compareTo("primarykey") == 0) {
                    primkey = true;
                    if (primaryKeyPresent == true) {
                        System.out.println("More than one primary key");
                        return;
                    } else {
                        primaryKeyPresent = true;    
                    }
                    
                }
            }
            Type attrtype1 = null;
            if(ATTRTYPE.toLowerCase().contains("(") && ATTRTYPE.toLowerCase().substring(0, ATTRTYPE.indexOf("(")).compareTo("varchar") == 0){
                length = Integer.parseInt( ATTRTYPE.substring(ATTRTYPE.indexOf("(")+1,ATTRTYPE.indexOf(")")) );
                attrtype1 = Type.Varchar;
            }
            else if(ATTRTYPE.toLowerCase().contains("(") && ATTRTYPE.toLowerCase().substring(0, ATTRTYPE.indexOf("(")).compareTo("char") == 0){
                length = Integer.parseInt( ATTRTYPE.substring(ATTRTYPE.indexOf("(")+1,ATTRTYPE.indexOf(")")) );
                attrtype1 = Type.Char;
            }
            else if(ATTRTYPE.toLowerCase().compareTo("double") == 0){
                
                attrtype1 = Type.Double;
            }
            else if(ATTRTYPE.toLowerCase().compareTo("int") == 0){
                attrtype1 = Type.Integer;
            }
            else if(ATTRTYPE.toLowerCase().compareTo("boolean") == 0){
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
        if (defaulttoken != null){

        
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
                found = i;
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
            if (tableCol.get(i).isUnique() || tableCol.get(i).isKey()) {
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
            print_display_info(i);
        }
        if(curr_cat.getTables().size() == 0){
            System.out.println("No tables to display");
        }
        
    }

    public void print_display_info(Table table){
        int numRecords = this.sM.getRecords_tablenumber(table.getNumber()).size();
        System.out.println(table.toString());    
        System.out.println("number of pages: " + table.getPagecount());
        System.out.println("number of records: " + numRecords);
    }

    public void select_statment(Table table){
        
        List<Record> allrec = sM.getRecords_tablenumber(table.getNumber());
        System.out.println("");
        for(Attribute i : table.getAttributes()){
            System.out.print(" | "+ i.getName());

        }
        System.out.println("");
        for(Record i : allrec){
            for(Object j : i.getValues()){
                System.out.print(" | " );
                if(j == null){
                    System.out.print("null");
                    continue;
                }
                else{
                    System.out.print(j);
                }
                
            }
            System.out.println("");
        }
    }

}