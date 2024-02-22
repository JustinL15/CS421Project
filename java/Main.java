import java.util.*; 
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
        
        List<Table> tablelist = new ArrayList<Table>();
        Catalog myCatalog = new Catalog(Integer.parseInt(args[2]),Integer.parseInt(args[1]),tablelist);
        Buffer mybuffer = new Buffer(myCatalog,args[2]);
        StorageManager sM = new StorageManager(mybuffer);
        Parser myParser = new Parser(sM);

        if(dbfound){
            System.out.println("Database Found\n");
            //get db catalog
            //myCatalog = Catalog(args[2]);
        }
        else{
            if(args.length != 3){
                System.out.println("Please print proper format:");
                System.out.println("run> arg1:path arg2:page size");
                System.exit(0);
            }
            System.out.println("Creating database");
            System.out.println("New db created successfully");  
            System.out.print("Page size: "+args[1]);
            System.out.println("Buffer size: "+args[2]);
            //create catalog

        }
        //create buffer, storage manager, parser

        System.out.println("----------------Starting Database Console---------------------");
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
                case "end", "stop", "quit":
                    breakflag = true;
                    break;
                case "select":
                    myParser.select_statment(myCatalog.getTableByName(arguments[2]));
                    ;
                case "create":
                    if(arguments[1] == "table"){
                        int lastindex = input.lastIndexOf(")");
                        int firstindex = input.indexOf("(");
                        //System.out.print(input.substring(firstindex+1,lastindex));
                        myParser.create_table(arguments[2], 0, input.substring(firstindex+1,lastindex));
                    }
                    break;
                case "alter":
                    //implement 
                    myParser.alter_table();
                case "drop":
                    myParser.drop_table(arguments[1]);
                    break;
                case "insert":
                    myParser.insert_values(null, args, arguments);
                    break;
                case "display":
                    if (arguments[1] == "info"){
                        myParser.print_display_info(myCatalog.getTableByName(arguments[2]));
                    }
                    if(arguments[1] ==  "schema"){
                        myParser.print_display_schema();
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

    }
    
}