parametrizeQuery <- function(query, parameters){
  
  require(data.table)
  
  expectedColumns = c("from", "to")
  stopifnot(ncol(parameters) == 2)
  stopifnot(all(colnames(parameters) %in% expectedColumns))
  
  for(parameter in parameters) {
    
    from = parameters$from
    to   = parameters$to
    
    query = str_replace_all(query, as.character(from),  as.character(to))
    
  }
  
  # Remove comments
  query = str_replace_all(query, "--.*(\r\n|\n)", "")
  
  # Remove break lines and carriage return and format excess of spaces
  query = str_replace_all(query, "(\r|\n|\t)", " ")
  str_replace_all(query, "([ ]+)", " ")
}
