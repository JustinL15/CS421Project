import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SecureDirectoryStream;

public class Main {

    ///arg1= page size, arg 2 = buffer size
    public static void main(String[] args) throws Exception{
        if (args.length < 3){
            System.out.println("Expected 3 arguments, got " + args.length);
            return;
            // path = Paths.get("").toAbsolutePath();
        }

        Path path = Path.of(args[0]);
        System.out.println(path.toString());
        boolean dbfound = Files.exists(path);

        if (!dbfound){
            System.out.println("Directory does not exist " + path.toString());
            return;
        }
        
        Catalog myCatalog;
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
                System.out.println("No file at " + catalogPath);
                return;
            }

            try {
                byte[] bytes = new byte[(int)catalogAccessFile.length()];
                catalogAccessFile.read(bytes);
                catalogAccessFile.close();
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                myCatalog = new Catalog(Integer.parseInt(args[2]), byteBuffer);
            } catch (IOException e) {
                System.out.println("IO Exception when reading catalog file at " + catalogPath);
                System.out.println(e);
                e.printStackTrace();
                return;
            }
            catch(BufferOverflowException e){
                System.out.println("Error reading Catalog.File may be corrupted or invalid");
                return;
            }
            catch(BufferUnderflowException e){
                System.out.println("Error reading Catalog.File may be corrupted or invalid");
                return;
            }
        }
        else{
            if(args.length != 3){
                System.out.println("Please print proper format:");
                System.out.println("run> arg1:path arg2:page size");
                System.exit(0);
            }
            System.out.println("Creating database");
            //create catalog

            ArrayList<Table> tablelist = new ArrayList<Table>();
            myCatalog = new Catalog(Integer.parseInt(args[2]),Integer.parseInt(args[1]),tablelist);

            try {
                Files.createFile(Path.of(path + File.separator + "catalog"));
                Files.createDirectory(Path.of(path + File.separator + "tables"));
            } catch (IOException e) {
                System.out.println("IO Exception when creating catalog file and tables directory at " + path);
                return;
            }

            System.out.println("New db created successfully");
            System.out.print("Page size: "+args[1]);
            System.out.println("Buffer size: "+args[2]);

        }
        //create buffer, storage manager, parser
        //Buffer mybuffer = new Buffer(myCatalog,args[2]);
        StorageManager sM = new StorageManager(myCatalog,path.toString());
        Parser myParser = new Parser(sM);

        System.out.println("----------------------- ---Starting Database Console----------------------------");
        System.out.println("   Please enter commands, enter help for commands, enter quit to shutdown the db");
        boolean breakflag = false;
        Scanner scanner = new Scanner(System.in);
        String command = "";
        while (!breakflag) { //program loop
            System.out.print("\nDB>");
            String input = scanner.nextLine();
            command += input; //INSTRUCTOR FIX
            if(!input.endsWith(";"))
                continue;
            input = command;
            String[] arguments = input.split(" ");
            switch (arguments[0]) {
                case ("help;"):
                    help_message(); //usage
                    break;
                case "quit;":
                    breakflag = true;

                    byte[] bytes = myCatalog.toBinary();
                    File catalogFile = new File(catalogPath.toString());
                    RandomAccessFile catalogAccessFile;

                    try {
                        catalogAccessFile = new RandomAccessFile(catalogFile, "rw");
                    } catch (FileNotFoundException e) {
                        System.out.println("No file at " + catalogPath);
                        return;
                    }

                    try {
                        catalogAccessFile.write(bytes);
                        catalogAccessFile.close();
                    } catch (IOException e) {
                        System.out.println("IOException when writing catalog at " + catalogPath);
                        return;
                    }

                    sM.buffer.purge();
                    break;
                case "select":
                try {
                    if(arguments[1].compareTo("*") == 0 && arguments[2].compareTo("from") == 0){
                        Table table = myCatalog.getTableByName(arguments[3].substring(0, arguments[3].length() - 1));
                        if (table == null){
                            System.out.println("No such table " + arguments[3].substring(0, arguments[3].length() - 1));
                            break;
                        }
                        List<Table> tables = new ArrayList<Table>();
                        tables.add(table);
                        myParser.select_statment(tables,null);
                    }
                    break;
                } catch (Exception e){
                    System.out.println(e.getMessage());
                }
                case "create":
                try{
                    if(input.endsWith(";") == false){
                        System.out.println("Bad input. type help for command list");
                        break;
                    }
                    if(arguments[1].compareTo("table") == 0){
                        int lastindex = input.lastIndexOf(")");
                        int firstindex = input.indexOf("(");
                        if(lastindex == -1 || firstindex == -1){
                            System.out.println("ERROR: create table format invalid");
                            break;
                        }
                        String tablename = arguments[2];
                        if(arguments[2].contains("(")){
                            tablename = arguments[2].substring(0, arguments[2].indexOf("("));
                        }
                        myParser.create_table(tablename, myCatalog.getTables().size(), input.substring(firstindex+1,lastindex));
                    }
                    System.out.println("SUCCESS");
                    break;
                } catch (Exception e){
                    System.out.println("ERROR");
                    System.out.println(e.getMessage());
                    break;
                }
                case "alter":
                    try{
                        if(arguments[1].compareTo("table") == 0){
                            if(myCatalog.getTableByName(arguments[2]) == null){
                                System.out.println("Table of name " + arguments[2] + " does not exist");
                                break;
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
                                if (arguments.length == 8) {
                                    defaultval = arguments[7];
                                }
                                myParser.add_table_column(myCatalog.getTableByName(arguments[2]), addattr,defaultval);

                                
                            }
                            if(arguments[3].compareTo("drop") == 0){
                                myParser.delete_table_column(myCatalog.getTableByName(arguments[2]), arguments[4].substring(0, arguments[4].length() - 1));
                            }
                            //myParser.alter_table();
                        }
                        System.out.println("SUCCESS");
                        break;
                    } catch (Exception e){
                        System.out.println(e.getMessage());
                        break;
                    }
                case "drop":
                try{
                    if(arguments[1].compareTo("table") == 0){
                        if(myCatalog.getTableByName(arguments[2].substring(0, arguments[2].length() - 1)) == null){
                            System.out.println("Table of name " + arguments[2] + " does not exist");
                            break;
                        }
                        myParser.drop_table(arguments[2].substring(0, arguments[2].length() - 1));
                    }
                    System.out.println("SUCCESS");
                    break;
                } catch (Exception e){
                    System.out.println("ERROR");
                }
                case "insert":
                try {
                    Catalog catalog = myCatalog;
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
                            myParser.insert_values(table.getName(), r);
                        }
                    }
                } catch (Exception e){
                    System.out.println(e.getMessage());
                    break;
                }
                case "display":
                try{
                    if (arguments[1].compareTo("info") == 0){
                        Table table = myCatalog.getTableByName(arguments[2].substring(0, arguments[2].length() - 1));
                        if (table == null){
                            System.out.println("No such table " + arguments[2]);
                            break;
                        }
                        myParser.print_display_info(table);
                    }
                    if(arguments[1].compareTo("schema;") == 0){
                        myParser.print_display_schema(myCatalog);
                    }
                    System.out.println("SUCCESS");
                    break;
                } catch (Exception e){
                    System.out.println("ERROR");
                    System.out.println(e);
                    break;
                }
                case "update":
                try{
                    parseUpdate(arguments, myParser);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    break;
                }
                default:
                    System.out.println("Bad input. type help for command list");
                    //help_message(); //usage
                    break;
            }
            command = "";
        }
        scanner.close();
    }
    public static void help_message(){
        System.out.println("all functions");
        System.out.println("select * from (table name); >> gets table");
        System.out.println("create table (table name)(attribute_name type,attribute_name2 tyoe,...); >> creates table with attributes");//good
        System.out.println("drop table (table name); >> drops table"); //works
        System.out.println("alter (table); --- >> alters table based on values given"); // need to test with split function
        System.out.println("insert values into (tablename); >> starts loop to insert values"); //select and record/page number not update
        System.out.println("display info (table name); >> display values in table"); //good
        System.out.println("display schema; >> display schema"); //good

    }

    private static void parseUpdate(String[] arguments, Parser parser) throws Exception {
        if (arguments.length < 6) {
            throw new Exception("Syntax error for update command: Not enough arguments");
        }
        if (!arguments[0].equals("update") || !arguments[2].equals("set") || !arguments[4].equals("=")) {
            throw new Exception("Syntax error for update command");
        }

        String conditions = "";
        if (arguments.length >= 8 && arguments[6].equals("where")) {
            for (int i = 7; i < arguments.length; i++) {
                conditions += " " + arguments[i];
            }
        }

        parser.update(arguments[1], arguments[3], arguments[5], conditions);
    }
    
}