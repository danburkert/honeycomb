#!/usr/bin/env Rscript

library("lattice")
library("plyr")
library("gplots")
args <- commandArgs(trailingOnly=T)
benchmark_file <- args[1]
pdf_filename <- args[2]
data <- read.table(benchmark_file, header=T, sep=" ")
data <- data[with(data, order(Table,Query, Clients,Timestep)), ]
pdf(file=pdf_filename)
ymax <- max(data$OPS)
ymiddle <- ymax / 2
bwplot(OPS~Table|sprintf("Clients: %02d",Clients)+Query,
       data=data,
       auto.key=T,
       main=sprintf("OPS by Table, Clients, & Query (%s)", Sys.time()),
       xlab.top="(Mean / Median / Standard Deviation)",
       par.settings = list(plot.symbol = list(col = "transparent")),
       ylab="Queries/Second",
       scales=list(x=list(rot=90)),
       panel=function(x, y,...){
         panel.grid(v=0,h=-1)
         panel.bwplot(x,y, ...)
         aligned_data <- data.frame(x=x,y=y)
         ddply(aligned_data, .(x),function(x){
           with(x,
                {
                  txt <- paste(round(mean(y)), "/", median(y), "/", round(sd(y)))
                  multiplier <- -1 * sign(median(y) - ymiddle)
                  offset <- multiplier * (0.09 * ymax + 1.5 * (quantile(y, probs=c(.99)) - median(y)))
                  q <- quantile(y, probs=c(.99))
                  panel.text(x[1], q + offset, label=txt, cex=0.5, srt=90)
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

color_seq <- seq(16, 16+table_count-1)
line_types <- 1:table_count
xyplot(OPS~Timestep|Query+sprintf("Clients: %02d", Clients),
       main= "OPS / Time by Table",
       data=data,
       type="l",
       col=color_seq,
       groups=Table,
       auto.key=T,
       lty=line_types,
       lwd=lwd,
       ylab="Queries/Second",
       xlab="Time",
       scales=list(x=list(rot=90)),
       key=list(text = list(as.character(paste("Table:", unique(data$Table)))),
                lines = list(lwd=lwd, lty=line_types, col=color_seq)),
       panel=panel)

color_seq <- seq(16, 16+client_count-1)
line_types <- 1:client_count
xyplot(OPS~Timestep|paste("Table:",Table)+Query,
       main= "OPS / Time by Clients",
       data=data,
       type="l",
       col=color_seq,
       groups=Clients,
       auto.key=T,
       lty=1:6,
       lwd=lwd,
       ylab="Queries/Second",
       xlab="Time",
       scales=list(x=list(rot=90)),
       key=list(text = list(as.character(paste("Clients:", unique(data$Clients)))),
                lines = list(lwd=lwd, lty=line_types, col=color_seq)),
       panel=panel)

dev.off()
