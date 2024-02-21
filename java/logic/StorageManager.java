import java.io.File;  // Import the File class
import java.io.IOException;  // Import the IOException class to handle errors

public class StorageManager {
    public void getRecord_primarykey(){

    }
    public int getPage_table(){
        return 0;
    }
    public void getRecords_tablenumber(int number){
        
    }
    public void insertRecord_table (Table table){
        /*
         * First check for existing pages
         * 
         * Then read each table page in order from the table file
         * 
         * If the record is not inserted, insert it into the last page
         * 
         * If needed split the page
         */
        int pagetest = getPage_table(); // Checks the pages of a table to detemine if any pages already exist

        if(pagetest == 0){
            String fileName = table.getNumber() + ".txt"; // Checks the table object  to determine its number and 
                                                          // creates a new file name with that number 
            File myObj = new File(fileName);
            // Creates a new page
            // Add record to page
            // Inserts the page into the file
            // Ends the function
        }
        else{
            while(/* Current page is not last page */){
                while(/* Current record is not last record */){
                    if(/* New record exists before curr record, insert it */){

                    }
                }
                if(/* Page becomes overfull */){
                    // Split the page, then end the function
                }
            }
        }
        if(/* Record is not inserted */){
            //Insert into last page, if overfull split and end the function
        }
    }
    
    public void deleteRecord_primarykey(){

    }
    public void updateRecord_primarykey(){
        
    }
    public void createTable(Table new_Table) {
    }
    
    public void dropTable(String name){    
    }
    
}