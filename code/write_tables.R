## Write tables in a db. ##
library(sqldf)

writeTables <- function(df = NULL, 
                        table_name = NULL, 
                        db = NULL, 
                        testing = FALSE) {
  # sanity check
  if(is.null(df) == TRUE) stop("Don't forget to provide a data.frame.")
  if(is.null(table_name) == TRUE) stop("Don't forget to provide a table name.")
  if(is.null(db) == TRUE) stop("Don't forget to provide a data base name.")
  
  cat('Storing data in a database: ')
  
  # creating db
  db_name <- paste0(db, ".sqlite")
  db <- dbConnect(SQLite(), dbname = db_name)
  
  # check if the table already already exists in the db
  if (table_name %in% dbListTables(db) == FALSE) { 
    dbWriteTable(db,
                 table_name,
                 df,
                 row.names = FALSE,
                 overwrite = TRUE)
  }
  else {
    dbWriteTable(db,
                 table_name,
                 df,
                 row.names = FALSE,
                 overwrite = TRUE)
  }
  
  # testing mode
  if(testing == TRUE) {
    # testing for the correct table name
    if (table_name %in% dbListTables(db)) {
      message(paste('The table', table_name, 'is in the db.'))
    }
    
    # testing for the table with data
    # and sample data
    x <- dbReadTable(db, table_name)
    if (is.data.frame(x) == TRUE) { 
      message(paste('There is a table with', nrow(x), 'records. The head is:'))
      print(head(x, 5))
    }
  } 

  dbDisconnect(db)
  cat('Done!\n')
}