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
    public static void main(String[] args) {
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


        } catch (Exception e) {
            System.out.println(e.getMessage());
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
            parser.print_display_info(table);
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
        if(arguments.length > 5 && arguments[1].equals("into") && arguments[3].equals("values") ) {
            Table table = catalog.getTableByName(arguments[2]);
            if (table == null) {
                throw new Exception("Table " + arguments[2] + " does not exist.");
            }
            List<List<String>> records = new ArrayList<>();
            int curIdx = 4;
            while (curIdx < arguments.length) {
                if (arguments[curIdx].charAt(0) == '(') {
                    List<String> rec = new ArrayList<>();
                    if (arguments[curIdx].charAt(arguments[curIdx].length() - 2) == ')') {
                        rec.add(arguments[curIdx].substring(1, arguments[curIdx].length() - 2));
                    } else {
                        rec.add(arguments[curIdx].substring(1));
                        curIdx++;
                        while (!(arguments[curIdx].charAt(arguments[curIdx].length() - 2) == ')')) {
                            rec.add(arguments[curIdx]);
                            curIdx++;
                        }
                        rec.add(arguments[curIdx].substring(0, arguments[curIdx].length() - 2));
                    }
                    records.add(rec);
                    curIdx++;
                } else {
                    throw new Exception("Error parsing insert command.");
                }
                for (List<String> record : records) {
                    parser.insert_values(arguments[2], record);
                }
            }
            System.out.println("SUCCESS");
        }
        throw new Exception("Error parsing insert command.");
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
        if(arguments[1].compareTo("*") == 0 && arguments[2].compareTo("from") == 0){
            Table table = myCatalog.getTableByName(arguments[3].substring(0, arguments[3].length() - 1));
            if (table == null){
                throw new Exception("No such table " + arguments[3].substring(0, arguments[3].length() - 1));
            }
            parser.select_statment(table);
        }
        System.out.println("SUCCESS");
    }
}