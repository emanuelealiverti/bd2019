# Esempio con pyspark
# NOTA BENE: controllare che le altre sessioni spark (ad esempio, con Rstudio server) siano chiuse.
# Ad esempio, cliccando su File _> quit session

# Lancia pyspark con comando --driver-memory="10g" per evitare i messaggi "Not enough space to cache"
df = spark.read.csv("/home/DATA/train.csv",inferSchema =True,header=True) 
df.take(4)
df.columns

#Usiamo un assembler per unirle
cols=df.columns[1:]
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=cols,outputCol="features")
# Now let us use the transform method to transform our dataset
df=assembler.transform(df)
df.select("features").show(truncate=False)
df=df.withColumnRenamed("# label", "y")

train, test = df.randomSplit([0.75, 0.25], seed=12345)

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol = "y", maxIter=10, regParam=0.3, elasticNetParam=0.0)
m0 = lr.fit(train)
print(m0.coefficients)

predm0 = m0.transform(test).select("y", "prediction")
from pyspark.mllib.evaluation import BinaryClassificationMetrics
out = BinaryClassificationMetrics(predm0.rdd)
out.areaUnderROC
