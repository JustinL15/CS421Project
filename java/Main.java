import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    ///arg1= page size, arg 2 = buffer size
    public static void main(String[] args){
        Path path = Paths.get("").toAbsolutePath();
        System.out.println(path.toString());
        boolean dbfound = Files.exists(path);
        
        Catalog myCatalog;

        if(dbfound){
            System.out.println("Database Found\n");
            //get db catalog (unfinished)
            String catalogLocation = path + File.separator + "catalog";
            File catalogFile = new File(catalogLocation);
            RandomAccessFile catalogAccessFile;

            try {
                catalogAccessFile = new RandomAccessFile(catalogFile, "r");
            } catch (FileNotFoundException e) {
                System.out.println("No file at " + catalogLocation);
                return;
            }

            try {
                byte[] bytes = new byte[(int)catalogAccessFile.length()];
                catalogAccessFile.read(bytes);
                catalogAccessFile.close();
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                myCatalog = new Catalog(Integer.parseInt(args[2]), byteBuffer);
            } catch (IOException e) {
                System.out.println("IO Exception when reading catalog file at " + catalogLocation);
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
                    break;
                case "select":
                    if(arguments[1] == "*" && arguments[2] == "from"){
                        Table table = myCatalog.getTableByName(arguments[2]);
                        if (table == null){
                            System.out.println("No such table " + arguments[2]);
                            break;
                        }
                        myParser.select_statment(table);
                    }
                    break;
                case "create":
                    if(arguments[1] == "table"){
                        int lastindex = input.lastIndexOf(")");
                        int firstindex = input.indexOf("(");
                        myParser.create_table(arguments[2], myCatalog.getTables().size(), input.substring(firstindex+1,lastindex));
                    }
                    break;
                case "alter":
                    if(arguments[1] == "table"){
                        if(arguments[3] == "add"){
                            int datalength =0;
                            Type attrtype1 = null;
                            String defaultval = null;
                            if(arguments[5].substring(0, arguments[5].indexOf("(")) == "VARCHAR"){
                                datalength = Integer.parseInt( arguments[5].substring(arguments[5].indexOf("("),arguments[5].indexOf(")")) );
                                attrtype1 = Type.Varchar;
                            }
                            if(arguments[5].substring(0, arguments[5].indexOf("(")) == "CHAR"){
                                datalength = Integer.parseInt( arguments[5].substring(arguments[5].indexOf("("),arguments[5].indexOf(")")) );
                                attrtype1 = Type.Char;
                            }
                            if(arguments[5] == "DOUBLE"){
                                
                                attrtype1 = Type.Double;
                            }
                            if(arguments[5] == "INT"){
                                attrtype1 = Type.Integer;
                            }
                            if(arguments[5] == "BOOLEAN"){
                                attrtype1 = Type.Boolean;
                            }
                            Attribute addattr = new Attribute(arguments[5],attrtype1,datalength,true,false,false);
                            if (arguments.length == 7) {
                                defaultval = arguments[6];
                            }
                            myParser.add_table_column(myCatalog.getTableByName(arguments[2]), addattr,defaultval);

                            
                        }
                        if(arguments[3] == "drop"){
                            myParser.delete_table_column(myCatalog.getTableByName(arguments[2]), arguments[4]);
                        }
                        //myParser.alter_table();
                    }
                    break;
                case "drop":
                    if(arguments[1] == "table"){
                        myParser.drop_table(arguments[2]);
                    }
                    break;

                case "insert":
                    if(arguments[1] == "into" && arguments[3] == "values"){
                        
                        String[] tupleArray = input.substring(input.indexOf("(")).split(",");
                        for(String i : tupleArray){
                            int lastindex = i.lastIndexOf(")");
                            int firstindex = i.indexOf("(");
                            String insertvals = i.substring(firstindex+1,lastindex);
                            String[] insertvalsarray = insertvals.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                            try {
                                myParser.insert_values(arguments[2], Arrays.asList(insertvalsarray));
                            } catch (Exception e) {
                                System.out.println(e);
                                break;
                            }
                        }
                    }
                    break;
                case "display":
                    if (arguments[1] == "info"){
                        Table table = myCatalog.getTableByName(arguments[2]);
                        if (table == null){
                            System.out.println("No such table " + arguments[2]);
                            break;
                        }
                        myParser.print_display_info(table);
                    }
                    if(arguments[1] ==  "schema"){
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