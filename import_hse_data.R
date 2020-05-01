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
               target_table = "dic_hse_tables", 
               delete_previous = TRUE)
}

# Import series
import_series <- function(){
  # tbls <- as_tibble(sophisthse_tables())
  tbls <- get_db_table(fin_db_path, "dic_hse_tables")

  r <- 1
  r_step <- 1
  n <- nrow(tbls)
  while(r <= n){
    sz <- sophisthse(tbls$table_name[r : min(r + r_step - 1, n)], output = "zoo", is_table = TRUE)

    print(str_c("Importing from: ", tbls$table_name[r : min(r + r_step - 1, n)], " indicators: ", str_c(names(sz), collapse = ",")))
    
    if(!is.null(warnings())){
      print("!")
      summary(warnings())
      assign("last.warning", NULL, envir = baseenv())
    }
    
    if(is.null(sz)){
      r <- r + r_step
      next
    }
    
    sz_time <- time(sz)
    if (inherits(sz_time, "yearmon")){
      freq <- 12
      d <- ceiling_date(as_date(sz_time), unit = "month") - 1
    } else if (inherits(sz_time, "yearqtr")){
      freq <- 4
      d <- ceiling_date(as_date(sz_time), unit = "quarter") - 1
    } else{
      freq <- 1
      d <- dmy(str_glue("31.12.{sz_time}"))
    }
    
    s <- as_tibble(sz) %>% 
      mutate(rep_date = d, rep_freq = freq)
    s <- s %>% 
      pivot_longer(!starts_with("rep_"), names_to = "tsname", values_to = "val")
    s_info <- as_tibble(attr(sz, "metadata"))
    
    export_to_db(db_path = fin_db_path, data = s, 
                 target_table = "hse_ts", 
                 delete_previous = FALSE)
    export_to_db(db_path = fin_db_path, data = s_info, 
                 target_table = "dic_hse_ts_info", 
                 delete_previous = FALSE)
    r <- r + r_step
    Sys.sleep(0.3)
  }
}
