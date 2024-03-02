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
import java.util.Scanner;

public class Main2 {
    public static void main(String[] args) {
        try {
            if (args.length < 3){
                System.out.println("Expected 3 arguments, got " + args.length);
                return;
            }
            Path path = Path.of(args[0]);
            System.out.println(path.toString());
            boolean dbfound = Files.exists(path);

            Catalog myCatalog;
            Path catalogPath = Path.of(path.toString() + File.separator + "catalog");
            boolean catalogFound = Files.exists(catalogPath);

            // If you need to change this do so
            myCatalog = initCatalog(path);

            StorageManager sM = new StorageManager(myCatalog,path.toString());
            Parser myParser = new Parser(sM);

            System.out.println("----------------------- ---Starting Database Console----------------------------");
            System.out.println("   Please enter commands, enter help for commands, enter quit to shutdown the db");
            
            Scanner scanner = new Scanner(System.in);
            try {
                startParsing(scanner, myParser);
            } catch (Exception e) {
                // handle exceptions that shouldn't break program here
            }
            scanner.close();


        } catch (Exception e) {
            // handle critical exceptions here
        }



    }

    // This function should initalize the catalog whether or not there is a catalog
    // creates database if not
    // Throws exception if catalog file is corrupted/missing somehow
    public static Catalog initCatalog(Path path) throws Exception{
        return null;
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
                        // TODO: catch and handle exceptions
                    }
                    break;
                case ("create"):
                    try {
                        parseCreate(arguments, parser);
                    } catch (Exception e) {
                        // TODO: catch and handle exceptions
                    }
                    break;
                case ("alter"):
                    try {
                        parseAlter(arguments, parser);
                    } catch (Exception e) {
                        // TODO: catch and handle exceptions
                    }
                    break;
                case ("drop"):
                    try {
                        parseDrop(arguments, parser);
                    } catch (Exception e) {
                        // TODO: catch and handle exceptions
                    }
                    break;
                case ("insert"):
                    try {
                        parseInsert(arguments, parser);
                    } catch (Exception e) {
                        // TODO: catch and handle exceptions
                    }
                    break;
                case ("display"):
                    display();
                    break;
                default:
                    //default cases, maybe print a error message?
                    break;
            }
        }
    }

    private static void help() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'help'");
    }

    private static void display() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'display'");
    }

    private static void parseInsert(String[] arguments, Parser parser) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseInsert'");
    }

    private static void parseDrop(String[] arguments, Parser parser) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseDrop'");
    }

    private static void parseAlter(String[] arguments, Parser parser) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseAlter'");
    }

    private static void parseCreate(String[] arguments, Parser parser) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseCreate'");
    }

    private static void parseSelect(String[] arguments, Parser parser) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parseSelect'");
    }
}