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
            // handle critical exceptions here
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