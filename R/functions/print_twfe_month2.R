print_twfe_month2 <- function(graphname, y, main_title, dat_list_name, legend_list, xlimit_l, xlimit_u){
  mod_list <- list()
  for (i in 1:length(dat_list_name)) {
    mod_list[[i]] <- readRDS(file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[i],"2.rds")))
    # Maybe exclude the points after xlimit_u and before xlimit_l - Better looking graphs but ylim_function2 is doing that already.
  }
  ylimit = ylim_function2(mod_list, dat_list_name, xlimit_l, xlimit_u)
  pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
  col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
  png(file.path(output_path,paste0("graphs/",graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
  par(cex.main = 2.5, cex.lab = 2, cex.axis = 2)
  #sep = 0.5, ref.line = -1
  plot(0, xlim = c(xlimit_l-0.1,xlimit_u+0.6), ylim = ylimit, 
       xlab = "Months", ylab = "", main = main_title, type = "n")
  grid()
  abline(h = 0)
  abline(v = -1, lty = 2)
  for (i in 1:length(dat_list_name)) {
    points(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$coefficients, pch = pch_list[i], col = col_list[i], cex=1.5) # , type = "b"
    segments(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, col = col_list[i], lwd = 2)
    arrows(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, angle = 90, code = 3, length = 0.05, col = col_list[i], lwd = 2)
    # Add the missing dots at x=-1
    points(-1+(i-1)*0.5, 0, pch = pch_list[i], col = col_list[i], cex=1.5)
  }
  legend("bottomleft", col = col_list, pch = pch_list, 
         legend = legend_list, cex = 1.5)
  dev.off()
  
  for (j in 1:length(dat_list_name)) {
    mod_list <- list()
    mod_list[[1]] <- readRDS(file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[j],"2.rds")))
    
    
    pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
    col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
    
    pch_list = pch_list[j]
    col_list = col_list[j]
    
    png(file.path(output_path,paste0("graphs/",graphname,y,"_", j,".png")), width = 640*4, height = 480*4, res = 200)
    par(cex.main = 2.5, cex.lab = 2, cex.axis = 2)
    #sep = 0.5, ref.line = -1
    plot(0, xlim = c(xlimit_l-0.1,xlimit_u+0.6), ylim = ylimit, 
         xlab = "Months", ylab = "", main = main_title, type = "n")
    grid()
    abline(h = 0)
    abline(v = -1, lty = 2)
    for (i in 1:1) {
      points(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$coefficients, pch = pch_list[i], col = col_list[i], cex=1.5) # , type = "b"
      segments(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, col = col_list[i], lwd = 2)
      arrows(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, angle = 90, code = 3, length = 0.05, col = col_list[i], lwd = 2)
      # Add the missing dots at x=-1
      points(-1+(i-1)*0.5, 0, pch = pch_list[i], col = col_list[i], cex=1.5)
    }
    legend("bottomleft", col = col_list, pch = pch_list, 
           legend = legend_list[j], cex = 1.5)
    dev.off()
  }
  
}