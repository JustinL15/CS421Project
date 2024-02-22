import java.util.*; 
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {

    ///arg 0 = path, arg1= page size, arg 2 = buffer size
    public static void main(String[] args){
        Path path = FileSystems.getDefault().getPath("..\\resources\\database.txt").toAbsolutePath();
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
            //create catalog

        }
        //create buffer, storage manager, parser

        System.out.println("----------------Starting Database Console---------------------");
        boolean breakflag = false;
        Scanner scanner = new Scanner(System.in);
        while (!breakflag) { //program loop
            System.out.println("--------------------------------------------------------------");
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
                    ;
                case "create table":
                    
                    break;
                case "alter table":
                    break;
                case "delete table":
                    break;
                case "insert record":
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