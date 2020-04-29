library("sophisthse")
library("dbrush")
library("lubridate")
library("tidyverse")
library("stringr")
library("purrr")
library("magrittr")

# Gather data from Sophist HSE database using sophisthse package
fin_db_path <- file.path("..", "Stress-testing", "BIRA", "Data2020", "fin_data.accdb")

# Import dictionary with indicators and their description
import_series_info <- function(){
  t <- as_tibble(series_info)
  export_to_db(db_path = fin_db_path, data = t, 
               target_table = "hse_dic_series_info", 
               delete_previous = TRUE)
}

# Import names of tables on HSE server
import_tables <- function(){
  tbls <- sophisthse_tables()
  t <- as_tibble(tbls)
  names(t) <- "table_name"
  export_to_db(db_path = fin_db_path, data = t, 
               target_table = "hse_tables", 
               delete_previous = TRUE)
}

# Import single serie
import_serie <- function(tsname){
  s_z <- sophisthse(tsname, output = "zoo")
  s <- as_tibble(t_z)
  s_info <- attr(t_z, "metadata")
  
  time_str <- time(s_z)
  
  if(str_detect(time_str[1], "^\\d{4}\\s+Q\\d{1}$")){
    y <- str_match(time_str, "\\d{4}")
    q <- str_match(time_str, "\\d{1}$")
    dd <- str_c(str_replace_all(q, c("^1$" = "31.03", "^2$" = "30.06", "^3$" = "30.09", "^4$" = "31.12")), y, sep = ".")
    time_date <- dmy(dd)
  }
  
  s$rep_date <- time_date
  list(s, s_info)
}
