#!/usr/bin/env Rscript

sum_info <- function(values){
  summary_info <- capture.output(summary(values))
  summary_info[1] <- paste(summary_info[1], "   StdDev")
  summary_info[2] <- paste(summary_info[2], "   ", signif(sd(values, na.rm=T)))
  mtext(summary_info[1], side=1, padj=6)
  mtext(summary_info[2], side=1, padj=8)
}

args <- commandArgs(trailingOnly=T)
input <- args[1]
output <- args[2]
build_number <- args[3]
rows <- read.csv(input, header=T)
x <- c(1:nrow(rows))
pdf(file=output)
layout(matrix(c(1, 1, 2, 3), 2, 2, byrow=T))
for(name in colnames(rows)) { 
  values <- rows[name][,]
  plot(x, values, type="b", xlab="build", ylab=name, main=sprintf("%s (build %s)", name, build_number))
  sum_info(values)
  boxplot(values, ylab="time")
  hist(values, xlab="time", main="")
}
dev.off()

