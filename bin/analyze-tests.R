#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly=T)
input <- args[1]
output <- args[2]
rows <- read.csv(input, header=T)
x <- c(1:nrow(rows))
par(mfrow=c(1,1))
t_rows <- t(rows)
pdf(file=output)
for(name in names(rows)) {  
  plot(x, t_rows[name,], type="b", xlab="build", ylab=paste(name, " time"))  
  boxplot(t_rows[name,], main=paste(name, " time"))
  hist(t_rows[name,], main=paste(name, " histogram"), xlab="time")
}
dev.off()