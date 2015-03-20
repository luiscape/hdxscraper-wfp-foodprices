# Script to fetch and prepare
# WFP data for HDX.
library(RCurl)
library(openxlsx)
library(RJSONIO)

# ScraperWiki helper. 
onSw <- function(l = NULL, p = "tool/", d = TRUE) {
  if (d) return(paste0(p, l))
  else return(l)
}

# Loading helper functions.
source(onSw('code/sw_status.R'))  # for changing status in SW
source(onSw('code/write_tables.R'))  # for writing db tables

###################
## Configuration ##
###################
FILE_PATH = onSw("data/wfp_food_prices_data")

# Date format should be:
# dd-mm-yyyy e.g. 18-03-2015
fetchWFPData <- function(l = NULL, try_date = FALSE) {
	# Checking path has been provided.
	if (is.null(l)) stop("Please privide path.")

	# Constructing URL.
  if (try_date == TRUE) {
    d = format(Sys.Date(), "%d-%m-%Y")
    u = paste0("http://vam.wfp.org/sites/data/WFPVAM_FoodPrices_", d, ".xlsx")
  }
  else u = "http://vam.wfp.org/sites/data/WFPVAM_FoodPrices_18-03-2015.xlsx"

	# Downloading file.
	xlsx_location = paste0(l, ".xlsx")
	download.file(u, xlsx_location, method="wget")
	d = read.xlsx(xlsx_location)
  
  # Returning data.
  return(d)
}

# Function to clean and transform
# the dataset.
cleanAndTransform <- function(df = NULL) {
  if (is.null(df)) stop("Please provide a data.frame")

  cat("Cleaning data...\n")
  
  # A column is coming with Chinese characters.
  # I'll be removing it for now, but it
  # seems to be caused by a problem on their
  # end, not in our end.
  df$mp_commoditySource <- NULL
  
  # End.
  return(df)
}

# Making assessment
makeAssessment <- function(df = NULL) {
  cat("Making assessment ... \n")
  a <- list(
      n_countries = length(unique(df$ADM0_NAME)),
      n_markets = length(unique(df$mkt_name)),
      n_adm1_units = length(unique(df$ADM1_NAME)),
      n_records = nrow(df)
    )
  sink(onSw("http/assessment.json"))
  cat(toJSON(a))
  sink()
}

# Function to write output.
writeOutput <- function(df = NULL, csv = TRUE, db = TRUE, l = NULL) {
  cat("Writting output ... \n")
  if (!is.data.frame(df)) stop("Data provided isn't a data.frame.")
  if (csv) write.csv(df, paste0(l, ".csv"), row.names = FALSE)
  if (db) writeTables(df, "food_prices", "scraperwiki")
}

# ScraperWiki wrapper.
runScraper <- function(p, assessment = TRUE) {
  data <- fetchWFPData(p, try_date = FALSE)
  data <- cleanAndTransform(data)
  if (assessment) makeAssessment(data)
  writeOutput(df = data, l = p)
}

# runScraper(FILE_PATH)
# 
# Changing the status of SW.m
tryCatch(runScraper(FILE_PATH),
         error = function(e) {
           cat('Error detected ... sending notification.')
           system('mail -s "WFP food prices failed." luiscape@gmail.com, takavarasha@un.org')
           changeSwStatus(type = "error", message = "Scraper failed.")
           { stop("!!") }
         }
)
# If success:
changeSwStatus(type = 'ok')