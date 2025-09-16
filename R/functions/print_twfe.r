
print_twfe <- function(graphname, y, main_title, dat_list, dat_list_name, legend_list, xlimit_l, xlimit_u){
  #mod_list <- twfe(y,control1,control2,fe,dat_list)
  #Load the mod_list
  mod_list <- list()
  for (i in 1:length(dat_list)) {
    mod_list[[i]] <- readRDS(file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[i],".rds")))
  }
  pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
  col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
  if(length(dat_list) == 1){mod_list <- mod_list[[1]]}
  png(file.path(output_path,paste0(graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
  par(cex.main = 1.75, cex.lab = 1.5, cex.axis = 1.75)
  iplot(mod_list, sep = 0.5, ref.line = -1,
        xlab = '',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.1,xlimit_u+0.1), 
        ylim = ylim_function(mod_list, dat_list, xlimit_l, xlimit_u),
        col = col_list, pch = pch_list, cex=0.7) 
  legend("bottomleft", col = col_list, pch = pch_list, 
         legend = legend_list, cex = 1)
  dev.off()
}