import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main2 {
    public static void main(String[] args) throws Exception {
        try {
            if (args.length < 4){
                System.out.println("Expected 4 arguments, got " + args.length);
                return;
            }
            Path path = Path.of(args[0]);
            System.out.println(path.toString());

            // If you need to change this do so
            Catalog myCatalog = initCatalog(path, Integer.parseInt(args[1]), Integer.parseInt(args[2]), Boolean.parseBoolean(args[3]));

            StorageManager sM = new StorageManager(myCatalog,path.toString());
            Parser myParser = new Parser(sM);

            System.out.println("----------------------- ---Starting Database Console----------------------------");
            System.out.println("   Please enter commands, enter help for commands, enter quit to shutdown the db");
            
            Scanner scanner = new Scanner(System.in);
            try {
                startParsing(scanner, myParser);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            scanner.close();
            myParser.sM.buffer.purge();
            try (RandomAccessFile catalogFile = new RandomAccessFile(path.toString() + File.separator + "catalog", "rw")) {
                catalogFile.seek(0);
                catalogFile.write(myParser.sM.catalog.toBinary());
            }

        } catch (Exception e) {
            throw e;
        }
    }

    public static void initTrees(Path path, Boolean on) throws Exception {
        if(!on || Files.exists(Path.of(path.toString() + File.separator + "trees"))){
        }
        else{
            System.out.println("Creating B-Tree Directory");
            //create  B-Tree
            try {
                Files.createDirectory((Path.of(path + File.separator + "trees")));
            } catch (IOException e) {
                throw new Exception("IO Exception when creating trees directory at " + path);
            }
            System.out.println("New B-Tree Directory created successfully");
        }
    }

    // This function should initalize the catalog whether or not there is a catalog
    // creates database if not
    // Throws exception if catalog file is corrupted/missing somehow
    public static Catalog initCatalog(Path path, int pageSize, int bufferSize, boolean indexing) throws Exception {
        boolean dbfound = Files.exists(path);
        if (!dbfound){
            throw new Exception("Directory does not exist " + path.toString());
        }
        Path catalogPath = Path.of(path.toString() + File.separator + "catalog");
        boolean catalogFound = Files.exists(catalogPath);
        if(catalogFound){
            System.out.println("Catalog Found\n");
            //get db catalog (unfinished)
            File catalogFile = new File(catalogPath.toString());
            RandomAccessFile catalogAccessFile;

            try {
                catalogAccessFile = new RandomAccessFile(catalogFile, "r");
            } catch (FileNotFoundException e) {
                throw new Exception("No file at " + catalogPath);
            }

            try {
                byte[] bytes = new byte[(int)catalogAccessFile.length()];
                catalogAccessFile.read(bytes);
                catalogAccessFile.close();
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                return new Catalog(bufferSize, byteBuffer, indexing);
            } catch (IOException e) {
                throw new Exception("IO Exception when reading catalog file at " + catalogPath);
            } catch(BufferOverflowException e){
                if (catalogAccessFile.length() == 0) {
                    throw new Exception("Catalog file is empty.");
                }
                throw new Exception("Error reading Catalog.File may be corrupted or invalid");
            } catch(BufferUnderflowException e){
                throw new Exception("Error reading Catalog.File may be corrupted or invalid");
            }
        }
        else{
            System.out.println("Creating database");
            //create catalog

            ArrayList<Table> tablelist = new ArrayList<Table>();
            Catalog myCatalog = new Catalog(bufferSize,pageSize,tablelist,indexing);

            try {
                Files.createFile(Path.of(path + File.separator + "catalog"));
                Files.createDirectory(Path.of(path + File.separator + "tables"));
                initTrees(catalogPath, indexing);
            } catch (IOException e) {
                throw new Exception("IO Exception when creating catalog file and tables directory at " + path);
            }

            System.out.println("New db created successfully");
            System.out.print("Page size: "+ pageSize);
            System.out.println("Buffer size: "+ bufferSize);
            return myCatalog;
        }
    }

    public static void startParsing(Scanner scanner, Parser parser) throws Exception {
        String command = "";
        boolean breakflag = false;
        while (!breakflag) { //program loop
            System.out.print("\nDB>");
            String input = scanner.nextLine();
            if(!input.endsWith(";")) {
                input += " ";
            }
            command += input; //INSTRUCTOR FIX
            if(!input.endsWith(";"))
                continue;
            input = command;
            System.out.println(input);
            String[] arguments = input.split(" ");

            switch (arguments[0]) {
                case ("help;"):
                    help();
                    break;
                case ("quit;"):
                    return;
                case ("select"):
                    try {
                        parseSelect(arguments,input,parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case ("create"):
                    try {
                        parseCreate(arguments, input, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case ("alter"):
                    try {
                        parseAlter(arguments, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case ("drop"):
                    try {
                        parseDrop(arguments, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case ("insert"):
                    try {
                        parseInsert(arguments, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case ("display"):
                    try {
                        parseDisplay(arguments, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case "update":
                    try{
                        parseUpdate(arguments, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case "delete":
                    try{
                        parseDelete(arguments, parser);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                default:
                    System.out.println("Invalid command.");
                    break;
            }
            command = "";
        }
    }

    private static void parseDelete(String[] arguments, Parser parser) throws Exception {
        StorageManager sM = parser.sM;
        Catalog myCatalog = sM.catalog;

        convertAttributes(arguments, myCatalog);
        if (!arguments[1].equals("from")) {
            throw new Exception("Bad Command");
        }

        int length = arguments.length;
        arguments[length - 1] =  arguments[length - 1].substring(0, arguments[length - 1].length() - 1);

        String conditions = null;

        Table table = myCatalog.getTableByName(arguments[2]);
        if (table == null) {
            System.out.println("No such table " + arguments[2]);
        }

        if (arguments.length > 3 && arguments[3].equals("where")) {
            StringBuilder sb = new StringBuilder();
            for (int i = 4; i < arguments.length; i++) {
                sb.append(arguments[i]);
                sb.append(' ');
            }
            conditions = sb.toString();
        }

        parser.delete_statment(table, conditions);
        System.out.println("SUCCESS");
    }

    private static void help() {
        System.out.println("all functions");
        System.out.println("select * from (table name); >> gets table");
        System.out.println("create table (table name)(attribute_name type,attribute_name2 tyoe,...); >> creates table with attributes");//good
        System.out.println("drop table (table name); >> drops table"); //works
        System.out.println("alter (table); --- >> alters table based on values given"); // need to test with split function
        System.out.println("insert values into (tablename); >> starts loop to insert values"); //select and record/page number not update
        System.out.println("display info (table name); >> display values in table"); //good
        System.out.println("display schema; >> display schema"); //good
    }

    private static void parseDisplay(String[] arguments, Parser parser) throws Exception {
        Catalog myCatalog = parser.sM.catalog;
        if (arguments[1].equals("info")) {
            Table table = myCatalog.getTableByName(arguments[2].substring(0, arguments[2].length() - 1));
            if (table == null) {
                System.out.println("No such table " + arguments[2]);
            }
            else {
                parser.print_display_info(table);
            }
            return;
        }
        else if(arguments[1].equals("schema;")){
            parser.print_display_schema(myCatalog);
        }
        else{
            System.out.println("Bad Command");
        }
        System.out.println("SUCCESS");
        return;
    }
    private static void parseInsert(String[] arguments, Parser parser) throws Exception {
        Catalog catalog = parser.sM.catalog;
        try {
            if (arguments.length >= 5 && arguments[1].equals("into") && arguments[3].startsWith("values")) {
                Table table = catalog.getTableByName(arguments[2]);
                if (table == null) {
                    throw new Exception("Table " + arguments[2] + " does not exist.");
                }
                List<List<String>> records = new ArrayList<>();
                StringBuilder values = new StringBuilder();
                for (int i = 4; i < arguments.length; i++) { // Start from index 4
                    values.append(' ');
                    values.append(arguments[i]);
                }
                String icommand = values.toString();
                boolean open = false;
                List<String> record = new ArrayList<>();
                StringBuilder word = new StringBuilder();
                for (int i = 0; i < icommand.length(); i++) {
                    if (open == false && icommand.charAt(i) == '(') {
                        open = true;
                    } else if (open == true) {
                        switch (icommand.charAt(i)) {
                            case '(':
                                throw new Exception("Error parsing insert command.");
                            case ')':
                                if (word.length() != 0) {
                                    record.add(word.toString());
                                    word = new StringBuilder();
                                }
                                records.add(record);
                                record = new ArrayList<>();
                                open = false;
                                break;
                            case ';':
                                break;
                            case ' ':
                            case '\n':
                                if (word.length() != 0) {
                                    record.add(word.toString());
                                    word = new StringBuilder();
                                }
                                break;
                            case '"':
                                i++;
                                while (icommand.charAt(i) != '"') {
                                    word.append(icommand.charAt(i));
                                    i++;
                                }
                                if (icommand.charAt(i + 1) != ' ' && icommand.charAt(i + 1) != ',') {
                                    throw new Exception(" Error parsing insert command.");
                                }
                                break;
                            default:
                                word.append(icommand.charAt(i));
                                break;
                        }
                    }
                }
                for (List<String> r : records) {
                    parser.insert_values(table.getName(), r);
                }
            }
            System.out.println("SUCCESS");
        } catch (Exception e) {
            throw e;
        }
    }

    private static void parseDrop(String[] arguments, Parser parser) throws Exception {
        Catalog myCatalog = parser.sM.catalog;
        if(arguments[1].compareTo("table") == 0){
            if(myCatalog.getTableByName(arguments[2].substring(0, arguments[2].length() - 1)) == null){
                throw new Exception("Table of name " + arguments[2] + " does not exist");
            }
            parser.drop_table(arguments[2].substring(0, arguments[2].length() - 1));
        }
        System.out.println("SUCCESS");
    }

    private static void parseAlter(String[] arguments, Parser parser) throws Exception {
        Catalog myCatalog = parser.sM.catalog;
        if(arguments[1].equals("table")){
            if(myCatalog.getTableByName(arguments[2]) == null){
                throw new Exception("Table of name " + arguments[2] + " does not exist");
            }
            if(arguments[3].equals("add")){
                int datalength = 0;
                Type attrtype1 = null;
                String defaultval = null;
                if(arguments[5].toLowerCase().contains("(") && arguments[5].toLowerCase().substring(0, arguments[5].indexOf("(")).compareTo("varchar") == 0){
                    datalength = Integer.parseInt( arguments[5].substring(arguments[5].indexOf("(")+1,arguments[5].indexOf(")")) );
                    attrtype1 = Type.Varchar;
                }
                else if(arguments[5].toLowerCase().contains("(") && arguments[5].toLowerCase().substring(0, arguments[5].indexOf("(")).compareTo("char") == 0){
                    datalength = Integer.parseInt( arguments[5].substring(arguments[5].indexOf("(")+1,arguments[5].indexOf(")")) );
                    attrtype1 = Type.Char;
                }
                else if(arguments[5].substring(0, arguments[5].length() - 1).compareTo("double") == 0){     
                    attrtype1 = Type.Double;
                }
                else if(arguments[5].substring(0, arguments[5].length() - 1).compareTo("integer") == 0){
                    attrtype1 = Type.Integer;
                }
                else if(arguments[5].substring(0, arguments[5].length() - 1).compareTo("boolean") == 0){
                    attrtype1 = Type.Boolean;
                }
                else {
                    System.out.println("Invalid data type " + arguments[5].substring(0, arguments[5].length() - 1));
                }
                Attribute addattr = new Attribute(arguments[4],attrtype1,datalength,true,false,false); // doesn't account for unique, notnull, key
                if (arguments.length == 8) {
                    defaultval = arguments[7];
                }
                parser.add_table_column(myCatalog.getTableByName(arguments[2]), addattr,defaultval);

                
            }
            if(arguments[3].compareTo("drop") == 0){
                parser.delete_table_column(myCatalog.getTableByName(arguments[2]), arguments[4].substring(0, arguments[4].length() - 1));
            }
        }
        System.out.println("SUCCESS");
    }

    private static void parseCreate(String[] arguments, String input, Parser parser) throws Exception {
        Catalog myCatalog = parser.sM.catalog;
        if(arguments[1].compareTo("table") == 0){
            int lastindex = input.lastIndexOf(")");
            int firstindex = input.indexOf("(");
            if(lastindex == -1 || firstindex == -1){
                throw new Exception("ERROR: create table format invalid");
            }
            String tablename = arguments[2];
            if(arguments[2].contains("(")){
                tablename = arguments[2].substring(0, arguments[2].indexOf("("));
            }
            parser.create_table(tablename, myCatalog.getTables().size(), input.substring(firstindex+1,lastindex));
        }
        System.out.println("SUCCESS");
    }

    private static void parseSelect(String[] arguments, String argumentline, Parser parser) throws Exception {
        Catalog myCatalog = parser.sM.catalog; 
        convertAttributes(arguments, myCatalog);

        StringBuilder sb = new StringBuilder();
        sb.append(arguments[0]);
        for (int i = 1; i < arguments.length; i++) {
            sb.append(' ');
            sb.append(arguments[i]);
        }
        argumentline = sb.toString();

        int arg_counter = 1;
        List<String> columns = new ArrayList<String>();
        //columns
        if (arguments[1].equals("*") && arguments[2].equals("from")) {
            columns = null;
            arg_counter = 2;
        } else {
            while (!arguments[arg_counter].equals("from")) {
                String curr_val = arguments[arg_counter].trim(); 
                if (!curr_val.isEmpty()) {
                    if (curr_val.equals(",")) {
                        // Do nothing
                    } else if (curr_val.endsWith(",")) {
                        columns.add(curr_val.substring(0, curr_val.length() - 1));
                    } else {
                        columns.add(curr_val);
                    }
                }
                arg_counter++;
                if(arg_counter == arguments.length){
                    throw new Exception("Invalid select statement");
                }
            }
        }
        arg_counter++;
        //tables
        List<Table> tables = new ArrayList<Table>();
        String where = null;
        String orderby = null;
        String from = null;
        //System.out.println(arguments[arg_counter].charAt(arguments[arg_counter].length() - 1));
        // while(arguments[arg_counter] != "where" && arguments[arg_counter].charAt(arguments[arg_counter].length() - 1) != ';' ){
 
        String regexString = "from(.*?)where";
        String regexString2 =  "from(.*?)orderby";
        String regexString3 =  "from(.*?);";
        Pattern pattern = Pattern.compile(regexString);
        Pattern pattern2 = Pattern.compile(regexString2);
        Pattern pattern3 = Pattern.compile(regexString3);
	    Matcher matcher = pattern.matcher(argumentline);
        Matcher matcher2 = pattern2.matcher(argumentline);
        Matcher matcher3 = pattern3.matcher(argumentline);
        if(matcher.find()) {
            from = matcher.group(1);
            from = from.trim();
        }
        else if(matcher2.find()){
            from = matcher2.group(1);
            from = from.trim();
        }
        else if(matcher3.find()){
            from = matcher3.group(1);
            from = from.trim();
        }
        if(from == null ){
            throw new Exception("Invalid select statement");
        }
        String[] tablenames = from.split(",");
        for ( String tablename : tablenames) {
            Table table;
            if(tablename.equals("")){
                throw new Exception("please format with commas");
            }
            else if(tablename.trim().contains(" ")){
                throw new Exception("please format with commas");
            }
            else{
                table = myCatalog.getTableByName(tablename.trim());
                if (table == null){
                    throw new Exception("Table of name " + tablename + " does not exist");
                }
                tables.add(table);
                }
        }

        if(tables.size() == 0){
            throw new Exception("No valid tables in statement");
        }
        //where
        regexString = "where(.*?)orderby";
        regexString2 =  "where(.*?);";
        pattern = Pattern.compile(regexString);
        pattern2 = Pattern.compile(regexString2);
	    // text contains the full text that you want to extract data
	    matcher = pattern.matcher(argumentline);
        matcher2 = pattern2.matcher(argumentline);
        if(matcher.find()) {
            //System.out.println(matcher.group(1));
            where = matcher.group(1);
            where = where.trim();
        }
        else if(matcher2.find()){
            //System.out.println(matcher2.group(1));
            where = matcher2.group(1);
            where = where.trim();
        }

        regexString = "orderby(.*?);";
        pattern = Pattern.compile(regexString);
        matcher = pattern.matcher(argumentline);
        if(matcher.find()) {
            //System.out.println(matcher.group(1));
            orderby = matcher.group(1);
            orderby = orderby.trim();
        }

        parser.select_statment(tables,columns,where,orderby);

        System.out.println("SUCCESS");
    }

    private static void parseUpdate(String[] arguments, Parser parser) throws Exception {
        convertAttributes(arguments, parser.sM.catalog);

        if (arguments.length < 6) {
            throw new Exception("Syntax error for update command: Not enough arguments");
        }
        if (!arguments[0].equals("update") || !arguments[2].equals("set") || !arguments[4].equals("=")) {
            throw new Exception("Syntax error for update command");
        }

        if (arguments[arguments.length-1].endsWith(";")) {
            arguments[arguments.length-1] = arguments[arguments.length-1].substring(0, arguments[arguments.length-1].length() - 1);
        }
        String conditions = "";
        if (arguments.length >= 8 && arguments[6].equals("where")) {
            for (int i = 7; i < arguments.length; i++) {
                conditions += " " + arguments[i];
            }
        }
        conditions = conditions.strip();

        parser.update(arguments[1], arguments[3], arguments[5], conditions);
        System.out.println("SUCCESS");
    }

    public static String[] convertAttributes(String[] arguments, Catalog catalog) throws Exception {
        List<Table> tables = new ArrayList<>();
        for (int i = 0; i < arguments.length;) {
            while (i < arguments.length && !arguments[i].equals("from")) {
                i++;
            }
            while (i < arguments.length && !arguments[i + 1].equals("where") && !arguments[i + 1].equals("orderby")) {
                String[] tableargs = arguments[i + 1].split(",");
                for (String arg : tableargs) {
                    tables.add(convertStringTable(arg, catalog));   
                }
                if (arguments[i + 1].endsWith(";")) {
                    break;
                } else {
                    i++;
                }
            }
            break;
        }

        boolean attrnext = true;
        if (arguments[0].toLowerCase().equals("update")) {
            attrnext = false;
            tables.add(convertStringTable(arguments[1], catalog));
        }
        for (int i = 1; i < arguments.length; i++) {
            switch (arguments[i].toLowerCase()) {
                case "from":
                    attrnext = false;
                    break;
                case "where":
                    attrnext = true;
                    break;
                case "orderby":
                    attrnext = true;
                    break;
                case "set":
                    attrnext = true;
                    break;
                case "and":
                case "or":
                    break;
                default:
                    boolean isAlpha = Character.isAlphabetic(arguments[i].charAt(0));
                    boolean alreadyTabled = false;
                    for (Table table: tables) {
                        if (arguments[i].startsWith(table.getName() + ".")) {
                            alreadyTabled = true;
                            break;
                        }
                    }
                    if (attrnext && isAlpha && !alreadyTabled) {
                        String curAttr = arguments[i];
                        if (curAttr.endsWith(";")) {
                            curAttr = curAttr.substring(0, curAttr.length() - 1);
                        }
                        else if (curAttr.endsWith(",")) {
                            curAttr = curAttr.substring(0, curAttr.length() - 1);
                        }
                        String tableName = null;
                        for (Table table: tables) {
                            for (Attribute attr: table.getAttributes()) {
                                if (attr.getName().equals(table.getName() + "." + curAttr)) {
                                    if (tableName == null) {
                                        tableName = table.getName();
                                    } else {
                                        throw new Exception(curAttr + " is ambiguous");
                                    }
                                }
                            }
                        }
                        if (tableName == null) {
                            throw new Exception("Attribute " + curAttr + " does not exist");
                        }
                        if (arguments[i].endsWith(";")) {
                            arguments[i] = tableName + "." + curAttr + ";";
                        } else {
                            arguments[i] = tableName + "." + curAttr;
                        }
                    }
                    break;
            }
        }
        return arguments;
    }

    public static Table convertStringTable(String name, Catalog catalog) throws Exception {
        if (name.endsWith(",") || name.endsWith(";")) {
            String tablename = name.substring(0, name.length() - 1);
            Table table = catalog.getTableByName(tablename);
            if (table == null) {
                throw new Exception("Table of name " +  tablename + " does not exist");
            } else {
                return table;
            }
        } else {
            Table table = catalog.getTableByName(name);
            if (table == null) {
                throw new Exception("Table of name " +  name + " does not exist");
            } else {
                return table;
            }
        }
    }
}