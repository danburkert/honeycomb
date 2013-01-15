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
       par.settings = list(plot.symbol = list(col = "transparent")),
       panel=function(...){ 
         panel.grid(v=0,h=-1) 
         panel.bwplot(...)
         })
xyplot(Count~Timestep|Query+paste("Clients:", Clients), 
       data=data, 
       type="p", 
       pch=15:16,
       col=12:13,
       groups=Table,
       auto.key=T,
       key=list(text = list(as.character(paste("Table:", unique(data$Table)))),
                points = list(pch = 15:16, col = 12:13)),
       panel=function(...){ 
         panel.grid(v=-1,h=-1)
         panel.xyplot(...)
         })
xyplot(Count~Timestep|paste("Table:",Table)+Query, 
       data=data, 
       type="o", 
       pch=15:17,
       col=12:14,
       groups=paste("Clients:",Clients), 
       auto.key=T,
       key=list(text = list(as.character(paste("Client:", unique(data$Clients)))),
                points = list(pch = 15:17, col = 12:14)),
       panel=function(...){ 
         panel.grid(v=-1,h=-1)
         panel.xyplot(...)})
textplot(data_summary)
dev.off()
