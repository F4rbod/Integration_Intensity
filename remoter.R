install.packages("remoter")
library(remoter)

client(port = 55556, )
server
remoter::relay(addr='d0147',verbose = T,sendport = 55555)