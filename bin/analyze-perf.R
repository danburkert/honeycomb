library("lattice")
library("plyr")
library("gplots")
data <- read.table("bench.ssv", header=T, sep=" ")
data <- data[with(data, order(Table,Query, Clients,Timestep)), ]
data_summary <- daply(data, .(Table, Clients, Query), function(x) {
  out <- capture.output(summary(x$Count))
  info <- paste("Table:", x$Table[1], "Clients:", x$Clients[1], "Query:", x$Query[1])
  paste(info, "\n", out[1],"\n", out[2], "\n")
})

pdf(file="test.pdf")
bwplot(Count~Table|paste("Clients:",Clients)+paste("Query:",Query), 
       data=data, 
       auto.key=T,
       main="Plot (Mean/Median/Standard Deviation)",
       par.settings = list(plot.symbol = list(col = "transparent")),
       panel=function(x, y,...){ 
         panel.grid(v=0,h=-1) 
         panel.bwplot(x,y, ...)
         aligned_data <- data.frame(x=x,y=y)
         ddply(aligned_data, .(x),function(x){
           with(x, 
                {
                  txt <- paste(round(mean(y)), "/", median(y), "/", round(sd(y)))
                  panel.text(x[1], 100 + quantile(y, probs=c(.75)), label=txt)
                 })
           1
         })
       })
cex <- 0.5
lwd <- 2
table_count <- length(unique(data$Table))
client_count <- length(unique(data$Clients))
color_seq <- seq(12, 12+table_count-1)
panel <- function(...){ 
         panel.grid(v=-1,h=-1)
         panel.xyplot(...)
         }
line_types <- 1:table_count
xyplot(Count~Timestep|Query+paste("Clients:", Clients), 
       data=data, 
       type="l", 
       col=color_seq,
       groups=Table,
       auto.key=T,
       lty=line_types,
       lwd=lwd,
       key=list(text = list(as.character(paste("Table:", unique(data$Table)))),
                lines = list(lty=line_types, col=color_seq)),
       panel=panel)

color_seq <- seq(12, 12+client_count-1)
line_types <- 1:client_count
xyplot(Count~Timestep|paste("Table:",Table)+Query, 
       data=data, 
       type="l", 
       col=color_seq,
       groups=paste("Clients:",Clients), 
       auto.key=T,
       lty=1:6,
       lwd=3,
       key=list(text = list(as.character(paste("Client:", unique(data$Clients)))),
                lines = list(lty=line_types, col=color_seq)),
       panel=panel)
 
textplot(data_summary)
dev.off()
