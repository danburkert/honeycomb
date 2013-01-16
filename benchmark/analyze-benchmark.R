#!/usr/bin/env Rscript

library("lattice")
library("plyr")
library("gplots")
args <- commandArgs(trailingOnly=T)
benchmark_file <- args[1]
pdf_filename <- args[2]
data <- read.table(benchmark_file, header=T, sep=" ")
data <- data[with(data, order(Table,Query, Clients,Timestep)), ]
data_summary <- daply(data, .(Table, Clients, Query), function(x) {
  out <- capture.output(summary(x$QPS))
  info <- paste("Table:", x$Table[1], "Clients:", x$Clients[1], "Query:", x$Query[1])
  paste(info, "\n", out[1],"\n", out[2], "\n")
})
pdf(file=pdf_filename)
ymax <- max(data$QPS)
ymiddle <- ymax / 2
bwplot(QPS~Table|paste("Clients:",Clients)+paste("Query:",Query),
       data=data,
       auto.key=T,
       main="QPS by Table, Clients, & Query",
       xlab.top="(Mean / Median / Standard Deviation)",
       par.settings = list(plot.symbol = list(col = "transparent")),
       ylab="Queries/Second",
       panel=function(x, y,...){
         panel.grid(v=0,h=-1)
         panel.bwplot(x,y, ...)
         aligned_data <- data.frame(x=x,y=y)
         ddply(aligned_data, .(x),function(x){
           with(x,
                {
                  txt <- paste(round(mean(y)), "/", median(y), "/", round(sd(y)))
                  multiplier <- -1 * sign(median(y) - ymiddle)
                  offset <- multiplier * (0.07 * ymax + (quantile(y, probs=c(.99)) - median(y)))
                  q <- quantile(y, probs=c(.99))
                  panel.text(x[1], q + offset, label=txt, cex=0.5)
                 })
           1
         })
       })
cex <- 0.5
lwd <- 2
table_count <- length(unique(data$Table))
client_count <- length(unique(data$Clients))
panel <- function(...){
         panel.grid(v=-1,h=-1)
         panel.xyplot(...)
         }

color_seq <- seq(12, 12+table_count-1)
line_types <- 1:table_count
xyplot(QPS~Timestep|Query+paste("Clients:", Clients),
       main= "QPS / Time by Clients",
       data=data,
       type="l",
       col=color_seq,
       groups=Table,
       auto.key=T,
       lty=line_types,
       lwd=lwd,
       ylab="Queries/Second",
       xlab="Time",
       key=list(text = list(as.character(paste("Table:", unique(data$Table)))),
                lines = list(lwd=lwd, lty=line_types, col=color_seq)),
       panel=panel)

color_seq <- seq(12, 12+client_count-1)
line_types <- 1:client_count
xyplot(QPS~Timestep|paste("Table:",Table)+Query,
       main= "QPS / Time by Table",
       data=data,
       type="l",
       col=color_seq,
       groups=paste("Clients:",Clients),
       auto.key=T,
       lty=1:6,
       lwd=lwd,
       ylab="Queries/Second",
       xlab="Time",
       key=list(text = list(as.character(paste("Client:", unique(data$Clients)))),
                lines = list(lwd=lwd, lty=line_types, col=color_seq)),
       panel=panel)

textplot(data_summary)
dev.off()
