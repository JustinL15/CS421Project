import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    ///arg1= page size, arg 2 = buffer size
    public static void main(String[] args){
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
            System.out.println("Database Found\n");
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

        System.out.println("----------------Starting Database Console---------------------");
        System.out.println("   Please enter commands, enter <quit> to shutdown the db");
        boolean breakflag = false;
        Scanner scanner = new Scanner(System.in);
        while (!breakflag) { //program loop
            System.out.println("\nDB>");
            String input = scanner.nextLine();
            String[] arguments = input.split(" ");
            switch (arguments[0]) {
                case ("help"):
                    help_message(); //usage
                    break;
                case "quit":
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
                    if(arguments[1].compareTo("*") == 0 && arguments[2].compareTo("from") == 0){
                        Table table = myCatalog.getTableByName(arguments[2]);
                        if (table == null){
                            System.out.println("No such table " + arguments[2]);
                            break;
                        }
                        myParser.select_statment(table);
                    }
                    break;
                case "create":
                    if(arguments[1].compareTo("table") == 0){
                        int lastindex = input.lastIndexOf(")");
                        int firstindex = input.indexOf("(");
                        myParser.create_table(arguments[2], myCatalog.getTables().size(), input.substring(firstindex+1,lastindex));
                    }
                    break;
                case "alter":
                    if(arguments[1].compareTo("table") == 0){
                        if(myCatalog.getTableByName(arguments[2]) == null){
                            System.out.println("Table of name " + arguments[2] + " does not exist");
                            continue;
                        }
                        if(arguments[3].compareTo("add") == 0){
                            int datalength =0;
                            Type attrtype1 = null;
                            String defaultval = null;
                            if(arguments[5].substring(0, arguments[5].indexOf("(")).compareTo("VARCHAR") == 0){
                                datalength = Integer.parseInt( arguments[5].substring(arguments[5].indexOf("("),arguments[5].indexOf(")")) );
                                attrtype1 = Type.Varchar;
                            }
                            if(arguments[5].substring(0, arguments[5].indexOf("(")).compareTo("CHAR") == 0){
                                datalength = Integer.parseInt( arguments[5].substring(arguments[5].indexOf("("),arguments[5].indexOf(")")) );
                                attrtype1 = Type.Char;
                            }
                            if(arguments[5].compareTo("DOUBLE") == 0){
                                
                                attrtype1 = Type.Double;
                            }
                            if(arguments[5].compareTo("INT") == 0){
                                attrtype1 = Type.Integer;
                            }
                            if(arguments[5].compareTo("BOOLEAN") == 0){
                                attrtype1 = Type.Boolean;
                            }
                            Attribute addattr = new Attribute(arguments[5],attrtype1,datalength,true,false,false);
                            if (arguments.length == 7) {
                                defaultval = arguments[6];
                            }
                            myParser.add_table_column(myCatalog.getTableByName(arguments[2]), addattr,defaultval);

                            
                        }
                        if(arguments[3].compareTo("drop") == 0){
                            myParser.delete_table_column(myCatalog.getTableByName(arguments[2]), arguments[4]);
                        }
                        //myParser.alter_table();
                    }
                    break;
                case "drop":
                    if(arguments[1].compareTo("table") == 0){
                        if(myCatalog.getTableByName(arguments[2]) == null){
                            System.out.println("Table of name " + arguments[2] + " does not exist");
                            continue;
                        }
                        myParser.drop_table(arguments[2]);
                    }
                    break;

                case "insert":
                    if(arguments[1].compareTo("into") == 0 && arguments[3].compareTo("values") == 0){
                        if(myCatalog.getTableByName(arguments[2]) == null){
                            System.out.println("Table of name " + arguments[2] + " does not exist");
                            continue;
                        }
                        String[] tupleArray = input.substring(input.indexOf("(")).split(",");
                        for(String i : tupleArray){
                            int lastindex = i.lastIndexOf(")");
                            int firstindex = i.indexOf("(");
                            String insertvals = i.substring(firstindex+1,lastindex);
                            String[] insertvalsarray = insertvals.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                            try {
                                try {
                                myParser.insert_values(arguments[2], Arrays.asList(insertvalsarray));
                            } catch (Exception e) {
                                System.out.println(e);
                                break;
                            }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    break;
                case "display":
                    if (arguments[1].compareTo("info") == 0){
                        Table table = myCatalog.getTableByName(arguments[2]);
                        if (table == null){
                            System.out.println("No such table " + arguments[2]);
                            break;
                        }
                        myParser.print_display_info(table);
                    }
                    if(arguments[1].compareTo("schema") == 0){
                        myParser.print_display_schema(myCatalog,path.toString());
                    }
                    break;
                
                default:
                    help_message(); //usage
                    break;
            }
            
        }
        scanner.close();
    }
    public static void help_message(){
        System.out.println("all functions");
        System.out.println("select * from (table name) >> gets table");
        System.out.println("create table (table name)(attribute_name type,attribute_name2 tyoe,...) >> creates table with attributes");
        System.out.println("drop table (table name) >> drops table");
        System.out.println("alter (table) --- >> alters table based on values given");
        System.out.println("insert values --- >> insert values");
        System.out.println("display info table >> display values in table");
        System.out.println("display schema >> display schema");

    }
    
}