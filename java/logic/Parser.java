import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;



public class Parser {
    StorageManager sM;
    public Parser(StorageManager StorageMan){
        sM = StorageMan;
    }

    public void create_table(String name, int number, String TableAttr) throws Exception{
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
                        throw new Exception("More than one primary key");
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
            else if(ATTRTYPE.toLowerCase().compareTo("integer") == 0){
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

    public void add_table_column(Table table, Attribute newAttr,String defaulttoken) throws Exception{
        
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
                        defaultval = false;
                    }
                    else{
                        System.out.println("Error incorrect value for Attribute type");
                        return;
                    }
                    break;
                case Char:
                    defaulttoken = cleanString(defaulttoken);
                    if (defaulttoken.length() == newAttr.getMaxLength()) {
                    } else {
                        System.out.println("Invalid length for char length " + newAttr.getMaxLength());
                    }
                    break;
                case Varchar:
                    defaulttoken = cleanString(defaulttoken);
                    if (defaulttoken.length() <= newAttr.getMaxLength()) {
                        defaultval = defaulttoken;
                    } else {
                        System.out.println("Invalid length for varchar length " + newAttr.getMaxLength());
                    }
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
                // attrlist.remove(i);
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
                            try {
                                cVals.add(Double.parseDouble(val));
                            } catch (NumberFormatException e) {
                                StringBuilder er = new StringBuilder();
                                er.append("row(");
                                er.append(values.get(0));
                                for (int j = 1; j < values.size(); j++) {
                                    er.append(' ');
                                    er.append(values.get(j));
                                }
                                er.append("): Invalid data type: expected (double) got (string).");
                                throw new Exception(er.toString());
                            }
                            break;
                        case Boolean:
                            cVals.add(Boolean.parseBoolean(val));
                            break;
                        case Char:
                            int cMax = tableCol.get(i).getMaxLength();
                            if (val.length() != cMax) {
                                StringBuilder er = new StringBuilder();
                                er.append("row(");
                                er.append(values.get(0));
                                for (int j = 1; j < values.size(); j++) {
                                    er.append(' ');
                                    er.append(values.get(j));
                                }
                                er.append("): char(" + cMax + ") can only accept " + cMax + " chars; \"" + val + "\" is " + val.length());
                                throw new Exception(er.toString());
                            }
                            cVals.add(val);
                            break;
                        case Varchar:
                            int vMax = tableCol.get(i).getMaxLength();
                            if (val.length() > vMax) {
                                StringBuilder er = new StringBuilder();
                                er.append("row(");
                                er.append(values.get(0));
                                for (int j = 1; j < values.size(); j++) {
                                    er.append(' ');
                                    er.append(values.get(j));
                                }
                                er.append("): varchar(" + vMax + ") can only accept up to " + vMax + " chars; \"" + val + "\" is " + val.length());
                                throw new Exception(er.toString());
                            }
                            cVals.add(val);
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
            if (tableCol.get(i).isKey()) {
                int res = sM.checkUnique(table, i, cVals.get(i));
                if (res != -1) {
                    StringBuilder er = new StringBuilder();
                    er.append("Duplicate primary key for row(");
                    er.append(values.get(0));
                    for (int j = 1; j < values.size(); j++) {
                        er.append(' ');
                        er.append(values.get(j));
                    }
                    er.append(')');
                    throw new Exception(er.toString());
                }
            } else if (tableCol.get(i).isUnique()) {
                if (sM.checkUnique(table, i, cVals.get(i)) != -1) {
                    throw new Exception("Attribute '" + table.getAttributes().get(i).getName() + "' has to be unique");
                }
            }
        }

        Record record = new Record(table, cVals);
        
        // convert data into this function
        sM.insertSingleRecord(table, record);
    }


    public void print_display_schema(Catalog curr_cat){
        System.out.println("database location: "+ sM.databaseLocation);
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

    public void printing_out_records(List<Record> records, Table template,boolean print) {
        System.out.println("-------");
        String vals ="";
        if(print){
            vals = template.getName()+".";
        }
        for (Attribute attribute : template.getAttributes()) {
            System.out.print("| " + vals+attribute.getName() + " ");
        }
        
        System.out.println("|");
        System.out.println("-------");

        for (Record record : records) {
            List<Object> values = record.getValues();
            for (Object value : values) {
                System.out.print("|" + value + " ");
            }
            System.out.println("|");
        }
    }

    public void printing_out_records(List<Record> records, List<String> columns,int size, Table template, boolean print) {
         boolean print_array[] = new boolean[size];
        System.out.println("-------");
        String vals ="";
        if(print){
            vals = template.getName()+".";
        }
        int x = 0;
        for (Attribute attribute : template.getAttributes()) {
            if( columns.contains(attribute.getName()) ){
                System.out.print("| " + vals+attribute.getName() + " ");     
                print_array[x] = true;
            }
            x = x + 1;
            
        }
        
        System.out.println("|");
        System.out.println("-------");

        for (Record record : records) {
            List<Object> values = record.getValues();
            x = 0;
            for (Object value : values) {
                if(print_array[x] == true){
                    System.out.print("|" + value + " ");
                }
                x = x +1;
            }
            System.out.println("|");
        }
    }


    public void select_statment(List<Table> tables, List<String> columns,String where, String order) throws Exception{
        List<Record> allrec =  sM.getRecords_tablenumber(tables.get(0).getNumber());
        List<Attribute> new_attr = new ArrayList<Attribute>();; 
        Table newtemplate = tables.get(0);
        List<Record> new_rec = new ArrayList<Record>();
        boolean print= false;
        boolean cart = false;
        String neworder = order;
        if(tables.size() > 1){
            new_attr =  tables.get(0).getAttributes();//adjust_attrbute_names(tables.get(0).getName(), tables.get(0).getAttributes());
            newtemplate =  new Table(tables.get(0).getName(), -1, new_attr, -1);
            for ( Record i : allrec) {
                List<Object> all_obj = new ArrayList<Object>(); 
                all_obj.addAll(i.getValues());

                Record newrecord = new Record(newtemplate, all_obj);
                new_rec.add(newrecord);
            }
            cart = true;
        }
        else{
            new_rec = allrec;
            print = true;
            if (order != null && order.startsWith(tables.get(0).getName()+".")){
                neworder = order.substring(tables.get(0).getName().length()+1);
            }
        }

        // should probably use new_rec instead of allrec in this loop
        for (int i = 1; i < tables.size(); i++) {
            List<Attribute> all_attr = new ArrayList<Attribute>(); 
            all_attr.addAll(new_rec.get(0).getTemplate().getAttributes());
            all_attr.addAll( tables.get(i).getAttributes());  // adjust_attrbute_names(tables.get(i).getName(), tables.get(i).getAttributes())   );
            newtemplate =  new Table(allrec.get(0).getTemplate().getName() +" x "+ tables.get(i).getName(), -1, all_attr, -1);

            List<Record> allrec_2 = sM.getRecords_tablenumber(tables.get(i).getNumber()); 
            new_rec = Cart_product(new_rec,allrec_2,newtemplate);
        }
        if(where != null){
            new_rec = where(new_rec, where, cart);
        }
        if(order != null){
            new_rec = orderby(newtemplate, neworder, "asc", new_rec);
        }
        if(columns == null){
            printing_out_records(new_rec, newtemplate, print);
        }
        else {
            String allin = newtemplate.getAttributes().toString(); 
            for (int i = 0; i < columns.size(); i++) {
                if(allin.contains(columns.get(i)) == false){
                    throw new Exception(columns.get(i)+ " not found");
                }
            }
            printing_out_records(new_rec, columns, new_rec.get(0).getValues().size(), newtemplate, print);
        }
    }

    // This is the delete command for the table
    public void delete_statment(Table table, String conditions) throws Exception {
        // We start by gathering all the records in the table
        List<Record> tableRecords = sM.getRecords_tablenumber(table.getNumber());
        // Then we collect the attributes
        List<Attribute> attr = table.getAttributes();

        // Goes through each record for the condition
        if (conditions != null) {
            tableRecords = where(tableRecords, conditions,false);
        }

        // This iterates through all the records
        for (Record record : tableRecords) {
            // This uses the delete Record function to remove the record. 
            Object primeKey = record.getPrimaryKey();
            sM.deleteRecord_primarykey(table, primeKey);

        }
    }
    
    private List<Record> Cart_product(List<Record> allrec_1, List<Record> allrec_2, Table template) {
        List<Record> new_list = new ArrayList<Record>();
        
        for ( Record i : allrec_1) {
            for ( Record j : allrec_2) {
                List<Object> all_obj = new ArrayList<Object>(); 
                all_obj.addAll(i.getValues());
                all_obj.addAll(j.getValues());
                Record newrecord = new Record(template, all_obj);
                new_list.add(newrecord);
            }
        }
        return new_list;
    }
    private List<Attribute> adjust_attrbute_names(String table_name, List<Attribute> attributes){
        List<Attribute> new_attr = new ArrayList<Attribute>(); 
        for( Attribute i : attributes){
            new_attr.add(new Attribute(table_name+"."+ i.getName(), i.getDataType(), i.getMaxLength(), i.isNotNull(), i.isKey(), i.isUnique()));
        }
        return new_attr;
    }

    private String cleanString(String s) {
        StringBuilder stringBuilder = new StringBuilder();
        int quotes = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                quotes++;
                if (quotes > 1) {
                    break;
                }
            } else {
                stringBuilder.append(c);
            }
        }
        return stringBuilder.toString();
    }

    public Integer update(String tableName, String attributeName, String value, String conditions) throws Exception {
        Table table = sM.catalog.getTableByName(tableName);
        if (table == null) {
            throw new Exception("Table of name " + tableName + " does not exist");
        }

        List<Record> records = sM.getRecords_tablenumber(table.getNumber());
        if (conditions.length() > 0) {
            records = where(records, conditions,false);
        }

        List<Attribute> attributes = table.getAttributes();
        Integer attributeIndex = null;
        Type attributeType = null;
        Integer keyIndex = null;
        for (Attribute attribute : attributes) {
            if (attribute.getName().equals(attributeName)) {
                attributeIndex = attributes.indexOf(attribute);
                attributeType = attribute.getDataType();
            }
            if (attribute.isKey()) {
                keyIndex = attributes.indexOf(attribute);
            }
        }
        if (attributeIndex == null) {
            throw new Exception("Attribute of name " + attributeName + " does not exist");
        }

        Object castedValue = null;
        switch (attributeType) {
            case Varchar, Char:
                if (LogicNode.isValidString(value)) {
                    castedValue = value.replace("\"", "");
                } else {
                    throw new Exception("Improper format for String attribute " + attributeName);
                }
                break;
            case Boolean:
                if (value.equals("true")) {
                    castedValue = true;
                }
                else if (value.equals("false")) {
                    castedValue = false;
                }
                else {
                    throw new Exception("Improper format for Boolean attribute " + attributeName);
                }
                break;
            case Double:
                if (LogicNode.isValidDouble(value)) {
                    castedValue = Double.parseDouble(value);
                }
                else {
                    throw new Exception("Improper format for Double attribute " + attributeName);
                }
                break;
            case Integer:
                if (LogicNode.isValidIntegerInRange(value)) {
                    castedValue = Integer.parseInt(value);
                }
                else {
                    throw new Exception("Improper format for Integer attribute " + attributeName);
                }
                break;
        }

        if (attributes.get(attributeIndex).isUnique() || attributes.get(attributeIndex).isKey()) {
            if (sM.checkUnique(table, attributeIndex, castedValue) != -1 || records.size() > 1) {
                throw new Exception("Attribute '" + attributes.get(attributeIndex).getName() + "' has to be unique");
            }
        }
        for (Record record : records) {
            record.getValues().set(attributeIndex, castedValue);
            sM.updateRecord_primarykey(record.getValues().get(keyIndex), record);
        }

        return records.size();
    }

    public List<Record> orderby(Table table, String attributeName, String order, List<Record> records) throws Exception {
        /** 
        Table table = sM.catalog.getTableByName(tableName);
        if (table == null) {
            throw new Exception("Table of name " + tableName + " does not exist");
        }*/
    
        int attributeIndex = -1;
        List<Attribute> attributes = table.getAttributes();
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getName().equals(attributeName)) {
                attributeIndex = i;
                break;
            }
        }
    
        if (attributeIndex == -1) {
            throw new Exception("Attribute '" + attributeName + "' does not exist in table '" + table.getName() + "'");
        }
    
        final int finalAttributeIndex = attributeIndex;
    
        Collections.sort(records, new Comparator<Record>() {
            @Override
            public int compare(Record r1, Record r2) {
                Object value1 = r1.getValues().get(finalAttributeIndex);
                Object value2 = r2.getValues().get(finalAttributeIndex);
    
                int result;
                if (value1 instanceof Integer && value2 instanceof Integer) {
                    result = ((Integer) value1).compareTo((Integer) value2);
                } else if (value1 instanceof Double && value2 instanceof Double) {
                    result = ((Double) value1).compareTo((Double) value2);
                } else if (value1 instanceof String && value2 instanceof String) {
                    result = ((String) value1).compareTo((String) value2);
                } else if (value1 instanceof Boolean && value2 instanceof Boolean) {
                    result = ((Boolean) value1).compareTo((Boolean) value2);
                } else {
                    throw new IllegalArgumentException("Unsupported data type for sorting");
                }
    
                if (order.equalsIgnoreCase("desc")) {
                    result = -result;
                }
                return result;
            }
        });
    
        return records;
    }

    public List<Record> where(List<Record> records, String conditions, boolean cart) throws Exception {
        List<Record> result = new ArrayList<>();
        LogicNode logicTree = LogicNode.build(conditions, cart);

        for (int i = 0; i < records.size(); i++){
            if(logicTree.evaluate(records.get(i))) {
                result.add(records.get(i));
            }
        }

        return result;
    }

    public static void main(String[] args) {
        List<Record> unsortedRecords = new ArrayList<>();
        Attribute a1 = new Attribute("Id", Type.Integer, 0, false, true, false);
        Attribute a2 = new Attribute("Name", Type.Varchar, 10, false, false, false);
        List<Attribute> aL = new ArrayList<>();
        aL.add(a1);
        aL.add(a2);
        Table t1 = new Table("test", 0, aL, 0);
        List<Table> tL = new ArrayList<>();
        tL.add(t1);
        Catalog c1 = new Catalog(100, 100, tL);
        StorageManager sM = new StorageManager(c1, "resources");
        Parser p = new Parser(sM);
        List<Object> v1 = new ArrayList<>();
        v1.add(0);
        v1.add("John");
        List<Object> v2 = new ArrayList<>();
        v2.add(1);
        v2.add("Tommy");
        List<Object> v3 = new ArrayList<>();
        v3.add(2);
        v3.add("Sarah");
        List<Object> v4 = new ArrayList<>();
        v4.add(3);
        v4.add("Cindy");
        List<Object> v5 = new ArrayList<>();
        v5.add(4);
        v5.add("Johnny");
        Record r1 = new Record(t1, v1);
        Record r2 = new Record(t1, v2);
        Record r3 = new Record(t1, v3);
        Record r4 = new Record(t1, v4);
        Record r5 = new Record(t1, v5);
        unsortedRecords.add(r1);
        unsortedRecords.add(r2);
        unsortedRecords.add(r3);
        unsortedRecords.add(r4);
        unsortedRecords.add(r5);
        try {
            p.orderby(c1.getTableByName("test"), "Name", "DESC", unsortedRecords);
            System.out.println();
            p.orderby(c1.getTableByName("test"), "Name", "ASC", unsortedRecords);
        } catch (Exception e) {
            e.printStackTrace();
        }
    
    }
}