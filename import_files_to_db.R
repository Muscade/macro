library("dbrush")
library("lubridate")
library("tidyverse")
library("stringr")
library("readxl")
library("purrr")
library("magrittr")

fin_db_path <- file.path(".", "Data2020", "fin_data.accdb")

clean_macro_cb_gks <- function(dirty_data, data_attributes) {
        tmp <- dirty_data %>%
                filter(!filter_na_rows(dirty_data)) %>% 
                filter(!is.na(indicator_num))
        
        tmp <- tmp %>% gather(names(tmp)[6:ncol(tmp)], key = "indicator1", value = "ind_value") %>%
                mutate(rep_year = as.integer(indicator1),
                       rep_tmp = str_replace_all(indicator2, c("^I квартал$" = "31.03",
                                                               "^II квартал$" = "30.06",
                                                               "^III квартал$" = "30.09",
                                                               "^IV квартал$" = "31.12",
                                                               "год" = "31.12")),
                       rep_date = dmy(str_c(rep_tmp, rep_year, sep = ".")),
                       rep_quarter = quarter(rep_date),
                       indicator_freq_m = if_else(indicator2 == "год", 12, 3),
                       indicator_name = indicator_name1
                       ) %>%
                select(-rep_tmp, -indicator1, -indicator2, -indicator_name2, -indicator_name1, -indicator_name)
        
        return(tmp)
}

import_macro_cb_gks <- function(){
        tmp <- load_excel_data(data_structure_file = "structure_macro_cb_gks_v1.0",
                               data_path = file.path(".", "Data2020"),
                               all_files = TRUE,
                               clean_data = clean_macro_cb_gks)
        print(tmp)
        
        return(tmp)
}

clean_macro_bira <- function(dirty_data, data_attributes) {
        tmp <- dirty_data %>%
                filter(!filter_na_rows(dirty_data))
        
        tmp <- tmp %>% 
                arrange(row_num) %>% 
                fill(country_name) %>% 
                mutate(
                       rep_date = data_date,
                       rep_quarter = quarter(rep_date)
                ) %>%
                select(-data_date)
        
        return(tmp)
}

import_macro_bira <- function(){
        tmp <- load_excel_data(data_structure_file = "structure_macro_bira_v1.0",
                               data_path = file.path(".", "Data2020"),
                               all_files = TRUE,
                               clean_data = clean_macro_bira)
        print(tmp)
        
        return(tmp)
}

# Интерфейс для выбора данных для загрузки
import_data <- function(h){
        options <- list(
                "Макропоказатели из сценария БР" = 
                        list("macro_cb_gks", import_macro_cb_gks),
                "Макропоказатели из БИР-аналитик" = 
                        list("macro_bira", import_macro_bira)
        )
        
        chosen_option <- select.list(names(options), graphics = TRUE)
        if (chosen_option != "") {
                c_opt <- options[[chosen_option]]
                d <- c_opt[[2]]()
        }
        
        export_to_db(db_path = fin_db_path, data = d, 
                     target_table = c_opt[[1]], 
                     delete_previous = TRUE)
}

import_data()