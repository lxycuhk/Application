# The code is trying to solve the log likelihood function of a probit model to predict the winning chance of Hong Kong Horse Race

wholedata=read.csv("Data2012.csv",header=T)

DrawST=wholedata[,"Drawing"]*wholedata[,"ST"]
Tumprank=wholedata[,"TrumpCard4"]*wholedata[,"TAveStdRank30"]
Rank=wholedata[,"Rank"]
varname=c("DateValue","Raceno","HAgeNC","DaySince","WtCarried","JWinPer",
          "AveSpeedRating","LastSpeedRating","Eta1",
          "EtaRise12","NewSByEta1","LastLogOdds","TWinPer","HWinPer",
          "Drawing")
applydata=cbind(wholedata[,varname],DrawST,Tumprank,Rank)
applydata=applydata[1:51654,]
databydate=split(applydata,as.factor(applydata[,1]))

beta=rep(0,15)
deltax=0.0001

for(s in 1: 5){
  
  xterm=rep(0,15)
  com1=rep(0,15)
  com2=rep(0,15)
  
  for(i in 1:nlevels(factor(applydata[,1]))){
    databyraceno=split(databydate[[i]],as.factor(databydate[[i]][,2]))
    
    for(j in 1:nlevels(factor(databydate[[i]][,2]))){
      data=as.matrix(databyraceno[[j]])
      datavar=data[,3:17]
      datavar1=data[data[,"Rank"]!=1,3:17]
      datavar2=data[data[,"Rank"]!=1&data[,"Rank"]!=2,3:17]
      
      tempx=data[data[,"Rank"]==1,3:17]+0.86*data[data[,"Rank"]==2,3:17]+0.65*data[data[,"Rank"]==3,3:17]
      xterm=xterm+tempx
      
      tempcom1=colSums(diag(as.vector(exp(datavar%*%beta)))%*%datavar)/sum(exp(datavar%*%beta))+
        0.86*colSums(diag(as.vector(exp(0.86*datavar1%*%beta)))%*%datavar1)/sum(exp(0.86*datavar1%*%beta))+
        0.65*colSums(diag(as.vector(exp(0.65*datavar2%*%beta)))%*%datavar2)/sum(exp(0.65*datavar2%*%beta))
      com1=com1+tempcom1                           
      
      tempcom2=(colSums(diag(as.vector(exp(datavar%*%beta)))%*%(datavar^2))*sum(exp(datavar%*%beta))-(colSums(diag(as.vector(exp(datavar%*%beta)))%*%datavar))^2)/((sum(exp(datavar%*%beta)))^2)+
        (0.86^2*colSums(diag(as.vector(exp(0.86*datavar1%*%beta)))%*%(datavar1^2))*sum(exp(0.86*datavar1%*%beta))-(0.86^2)*(colSums(diag(as.vector(exp(0.86*datavar1%*%beta)))%*%datavar1))^2)/((sum(exp(0.86*datavar1%*%beta)))^2)+
        (0.65^2*colSums(diag(as.vector(exp(0.65*datavar2%*%beta)))%*%(datavar2^2))*sum(exp(0.65*datavar2%*%beta))-(0.65^2)*(colSums(diag(as.vector(exp(0.65*datavar2%*%beta)))%*%datavar2))^2)/((sum(exp(0.65*datavar2%*%beta)))^2)    
      com2=com2+tempcom2  
    }
  }

  beta=beta+(xterm-com1)/(com2)
  
}
para1=beta