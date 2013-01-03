#!/usr/bin/env Rscript

rows <- read.csv(pipe('cat /dev/stdin'), header=F)
data <- t(rows[!is.na(rows)])

cat("time in milliseconds\n")
cat("Count: ", length(data), '\n')
cat("Mean:", mean(data), '\n')
cat("Standard Deviation:", sd(as.numeric(data)), '\n')
cat("Quantiles:", '\n')
print(quantile(data))
