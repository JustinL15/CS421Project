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
            if (args.length < 3){
                System.out.println("Expected 3 arguments, got " + args.length);
                return;
            }
            Path path = Path.of(args[0]);
            System.out.println(path.toString());

            // If you need to change this do so
            Catalog myCatalog = initCatalog(path, Integer.parseInt(args[1]), Integer.parseInt(args[2]));

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

    // This function should initalize the catalog whether or not there is a catalog
    // creates database if not
    // Throws exception if catalog file is corrupted/missing somehow
    public static Catalog initCatalog(Path path, int pageSize, int bufferSize) throws Exception {
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
                return new Catalog(bufferSize, byteBuffer);
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
            Catalog myCatalog = new Catalog(bufferSize,pageSize,tablelist);

            try {
                Files.createFile(Path.of(path + File.separator + "catalog"));
                Files.createDirectory(Path.of(path + File.separator + "tables"));
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
                        parseSelect(arguments, parser);
                    } catch (Exception e) {
                        throw e;
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
                        e.printStackTrace();
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
                default:
                    System.out.println("Invalid command.");
                    break;
            }
            command = "";
        }
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
        if(arguments[1].equals("schema;")){
            parser.print_display_schema(myCatalog);
        }
        System.out.println("SUCCESS");
        return;
    }

    private static void parseInsert(String[] arguments, Parser parser) throws Exception {
        Catalog catalog = parser.sM.catalog;
        try {
            if(arguments.length >= 5 && arguments[1].equals("into") && arguments[3].startsWith("values")) {
                Table table = catalog.getTableByName(arguments[2]);
                if (table == null) {
                    throw new Exception("Table " + arguments[2] + " does not exist.");
                }
                List<List<String>> records = new ArrayList<>();
                StringBuilder values = new StringBuilder();
                for (int i = 3; i < arguments.length; i++) {
                    values.append(' ');
                    values.append(arguments[i]);
                }
                String icommand = values.toString();
                boolean open = false;
                List<String> record = new ArrayList<>();
                StringBuilder word = new StringBuilder();
                for (int i = 0; i < icommand.length(); i++) {
                    if (open == false &&  icommand.charAt(i) == '(') {
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
                                records.add(record);;
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
                                if (icommand.charAt(i + 1) != ' ') {
                                    throw new Exception(" Error parsing insert command.");
                                }
                                break;
                            default:
                                word.append(icommand.charAt(i));
                                break;
                        }
                    }
                }
                for (List<String> r: records) {
                    parser.insert_values(table.getName(), r);
                }
            }
        } catch (Exception e){
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
        if(arguments[1].compareTo("table") == 0){
            if(myCatalog.getTableByName(arguments[2]) == null){
                throw new Exception("Table of name " + arguments[2] + " does not exist");
            }
            if(arguments[3].compareTo("add") == 0){
                int datalength =0;
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
                if (arguments.length == 7) {
                    defaultval = arguments[6];
                }
                parser.add_table_column(myCatalog.getTableByName(arguments[2]), addattr,defaultval);

                
            }
            if(arguments[3].compareTo("drop") == 0){
                parser.delete_table_column(myCatalog.getTableByName(arguments[2]), arguments[4].substring(0, arguments[4].length() - 1));
            }
        }
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

    private static void parseSelect(String[] arguments, Parser parser) throws Exception {
        Catalog myCatalog = parser.sM.catalog; 
        int arg_counter = 1;
        List<String> columns = new ArrayList<String>();
        //columns
        if(arguments[1].compareTo("*") == 0 && arguments[2].compareTo("from") == 0){
            columns = null;
            arg_counter = 2;
        }
        else{
            while(arguments[arg_counter] != "from" ){
                String curr_val = arguments[arg_counter];
                if(curr_val == ","){
                    continue;
                } 
                if (curr_val.substring(curr_val.length() - 1) == "," ){
                    columns.add(curr_val.substring(0, curr_val.length() - 1));
                }
                else if(curr_val.substring(0,1) == "," ){
                    columns.add(curr_val.substring(1, curr_val.length()));
                }
                else{
                    columns.add(arguments[arg_counter]);
                }
            }
            arg_counter = arg_counter +1;
        }
        arg_counter = arg_counter +1;
        List<Table> tables = new ArrayList<Table>();
        
        while(arguments[arg_counter] != "where" && arguments[arg_counter].charAt(arguments[arg_counter].length() - 1) != ';' ){
            String curr_val = arguments[arg_counter];
            Table table;
            String tableName;
                if(curr_val == "," || curr_val == ";"){
                    continue;
                } 
                if (curr_val.substring(curr_val.length() - 1).compareTo(",") == 0 || curr_val.substring(curr_val.length() - 1).compareTo(";") == 0){
                    tableName = curr_val.substring(0, curr_val.length() - 1);
                }
                else if(curr_val.substring(0,1).compareTo(",") == 0 || curr_val.substring(curr_val.length() - 1).compareTo(";") == 0){
                    tableName = curr_val.substring(1, curr_val.length());
                }
                else{
                    tableName = arguments[arg_counter];
                }

                table = myCatalog.getTableByName(tableName);
                if (table == null){
                    throw new Exception("Table of name " + tableName + " does not exist");
                }
                tables.add(table);
                break;
        
        }
        
        parser.select_statment(tables,columns);


        System.out.println("SUCCESS");
    }
}