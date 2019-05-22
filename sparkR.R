Sys.setenv(SPARK_HOME = "/home/spark")
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "", sparkConfig = list(spark.driver.memory = "10g",
                                  spark.num.executor = "16",
                                  spark.executor.memory = "3g"))
sparkR.conf()
#+++++++++++++
# Lettura dati
#+++++++++++++
csvPath = "../DATA/train.csv"
df = read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA")
str(df)
df=withColumnRenamed(df,"# label","y")
csvPath2 = "../DATA/test.csv"
df_test = read.df(csvPath2, "csv", header = "true", inferSchema = "true", na.strings = "NA")
df_test=withColumnRenamed(df_test,"# label","y")
pryr::mem_used()
#++++++++
# Modello
#++++++++
my_form = paste("y~", paste0("f",1:25,collapse="+"))
my_form = as.formula(my_form)
?spark.logit
m0 = spark.logit(dat = df, formula = my_form, family = "binomial")
summary(m0)
