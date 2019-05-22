rm(list=ls())
require(data.table)
dir("/home/DATA/PAMAP2_Dataset/Protocol/")
system("du -csh /home/DATA/PAMAP2_Dataset/Protocol/")

load_filter = function(path){
  train = fread(file = path)
  train[!is.na(V3)]
}

file_list = lapply(dir("/home/DATA/PAMAP2_Dataset/Protocol/",full.names = T), load_filter)
str(file_list)
df_comb = rbindlist(file_list)
rm(file_list)
gc()

df_comb = na.omit(df_comb)
df_comb = df_comb[,!c("V1"),with=F]
setnames(df_comb,"V3","y")
setnames(df_comb,"V2","activity")
df_comb$activity = factor(df_comb$activity)
pryr::mem_used()

head(df_comb)
paste0("V",4:54)
covariate = paste0("V",(4:54),collapse = "+")
f0 = as.formula(paste0("y~activity+",covariate))

#Stima/verifica
set.seed(1)
id_stima = sample(1:NROW(df_comb), .75*NROW(df_comb))
id_verifica = setdiff(1:NROW(df_comb),id_stima)

require(biglm)
m0 = biglm(formula = f0, data = df_comb[id_stima,])

predError_m0=((df_comb[,"y"][id_verifica] - predict(m0,newdata = df_comb[id_verifica,]))^2)
mean(predError_m0$y)

bss_measure = function(subs, what = c("aic", "err")){
  what = match.arg(what)
  covariate = paste0("V", subs, collapse = "+")
  f_tmp = as.formula(paste0("y~activity+",covariate))
  m_tmp = biglm(formula = f_tmp, data = df_comb[id_stima,])

  out = switch(what,
          aic = AIC(m_tmp),
          err = {
		  predError_mtmp =((df_comb[,"y"][id_verifica] - predict(m_tmp,newdata = df_comb[id_verifica,]))^2)
		  mean(predError_mtmp$y)
	  }
  )
  return(out)
}


bss_measure(c(4:5), what = "e")

#Quanti sono i modelli con 10 variabili?
choose(54,10)
require(arrangements)
set.seed(1)
# Ne prendiamo solo alcuni
model_list = lapply(1:parallel::detectCores(), 
		    function(i) combinations(x=4:54,n=10,nsample = 100,nitem = 100))
str(model_list)
parallel::detectCores()

require(snowfall)
sfInit(parallel = T,cpus = parallel::detectCores())
sfLibrary(data.table)
sfLibrary(biglm)
sfExport(list = c("df_comb","id_stima", "id_verifica","bss_measure"))
aic_combn = sfLapply(model_list, function(matr) apply(matr,1, bss_measure))
err_combn = sfLapply(model_list, function(matr) apply(matr,1, bss_measure, what = "err"))
sfStop()
lapply(aic_combn, min)
lapply(err_combn, min)

which.min(lapply(aic_combn, min))
# 1
which.min(aic_combn[[1]])
# 63
which.min(lapply(err_combn, min))
which.min(err_combn[[1]])

model_list[[1]][63,]

covariate = paste0("V", model_list[[1]][63,], collapse = "+")
f_tmp = as.formula(paste0("y~activity+",covariate))
m0 = biglm(formula = f_tmp, data = df_comb)
summary(m0)
