from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ChemPipeline-BatchETL") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== Step 1: Loading ESOL dataset ===")
url = "https://raw.githubusercontent.com/deepchem/deepchem/master/datasets/delaney-processed.csv"
pdf = pd.read_csv(url)
df = spark.createDataFrame(pdf)
print(f"Loaded {df.count()} molecules")
print("Columns:", df.columns)

print("\n=== Step 2: Renaming columns ===")
df = df.withColumnRenamed("Compound ID", "compound_id") \
       .withColumnRenamed("measured log solubility in mols per litre", "solubility") \
       .withColumnRenamed("Molecular Weight", "mol_weight") \
       .withColumnRenamed("Number of Rings", "num_rings") \
       .withColumnRenamed("Polar Surface Area", "polar_surface_area")

print("\n=== Step 3: Filling nulls with column mean ===")
for c in ["solubility", "mol_weight", "num_rings"]:
    mean_val = df.select(mean(col(c))).first()[0]
    df = df.fillna({c: round(mean_val, 4)})

print("\n=== Step 4: Feature engineering ===")
df = df.withColumn(
    "solubility_class",
    when(col("solubility") >= -1, "high")
    .when(col("solubility") >= -3, "medium")
    .otherwise("low")
)
df = df.withColumn(
    "is_heavy",
    when(col("mol_weight") >= 300, True).otherwise(False)
)

print("\n=== Step 5: Summary statistics ===")
df.select("solubility", "mol_weight", "num_rings").describe().show()

print("\n=== Step 6: Solubility class distribution ===")
df.groupBy("solubility_class").count().orderBy("count", ascending=False).show()

print("\n=== Step 7: Saving ML-ready dataset ===")
df.select(
    "compound_id", "smiles", "solubility", "solubility_class",
    "mol_weight", "is_heavy", "num_rings", "polar_surface_area"
).write.mode("overwrite").parquet("output/esol_ml_ready")

print("=== ETL Complete ===")
spark.stop()
