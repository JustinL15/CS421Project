import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

public class Main {

    ///arg 0 = path, arg1,page size
    public static void main(String[] args){
        Path path = FileSystems.getDefault().getPath("C:\\Users\\Blake\\Documents\\RIT Files\\Database Systems\\Database-System-Implementation-Project\\resources\\database.txt").toAbsolutePath();
        System.out.println(path.toString());
        boolean dbfound = Files.exists(path);

        if(dbfound){
            System.out.println("Database Found\n");
            //get db catalog
        }
        else{
            if(args.length != 2){
                System.out.println("Please print proper format:");
                System.out.println("run> arg1:path arg2:page size");
                System.exit(0);
            }
            System.out.println("Creating database");
            //create catalog

        }

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
                    break;
                case "create table":
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