import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("GeneratorDanychParquet") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

print("Generowanie zbioru danych...")

schema = StructType([
    StructField("id_pracownika", IntegerType(), False),
    StructField("imie", StringType(), True),
    StructField("nazwisko", StringType(), True),
    StructField("wiek", IntegerType(), True),
    StructField("dzial", StringType(), True),
    StructField("pensja", DoubleType(), True),
    StructField("data_zatrudnienia", DateType(), True),
    StructField("ocena_efektywnosci", IntegerType(), True)
])

# Zaawansowane dane testowe (15 zróżnicowanych rekordów)
data = [
    (1, "Jan", "Kowalski", 35, "IT", 8500.0, datetime.strptime("2020-03-15", "%Y-%m-%d").date(), 4),
    (2, "Anna", "Nowak", 28, "HR", 5500.0, datetime.strptime("2021-06-01", "%Y-%m-%d").date(), 5),
    (3, "Piotr", "Zieliński", 42, "IT", 12000.0, datetime.strptime("2015-11-10", "%Y-%m-%d").date(), 3),
    (4, "Katarzyna", "Szymańska", 31, "Finanse", 7200.0, datetime.strptime("2019-01-20", "%Y-%m-%d").date(), 4),
    (5, "Michał", "Woźniak", 25, "IT", 4800.0, datetime.strptime("2023-09-01", "%Y-%m-%d").date(), 2),
    (6, "Małgorzata", "Dąbrowska", 45, "Finanse", 9500.0, datetime.strptime("2012-05-14", "%Y-%m-%d").date(), 5),
    (7, "Tomasz", "Kozłowski", 38, "Logistyka", 6100.0, datetime.strptime("2018-07-19", "%Y-%m-%d").date(), 3),
    (8, "Agnieszka", "Jankowska", 29, "HR", 5800.0, datetime.strptime("2022-02-11", "%Y-%m-%d").date(), 4),
    (9, "Krzysztof", "Mazur", 50, "Logistyka", 8000.0, datetime.strptime("2010-04-01", "%Y-%m-%d").date(), 4),
    (10, "Barbara", "Kwiatkowska", 33, "IT", 9100.0, datetime.strptime("2017-08-24", "%Y-%m-%d").date(), 5),
    (11, "Paweł", "Krawczyk", 27, "Marketing", 5200.0, datetime.strptime("2023-01-10", "%Y-%m-%d").date(), 3),
    (12, "Ewa", "Piotrowska", 36, "Marketing", 6700.0, datetime.strptime("2020-10-05", "%Y-%m-%d").date(), 4),
    (13, "Janusz", "Grabowski", 55, "Dyrekcja", 18000.0, datetime.strptime("2005-01-01", "%Y-%m-%d").date(), 5),
    (14, "Magdalena", "Król", 24, "HR", 4500.0, datetime.strptime("2024-01-15", "%Y-%m-%d").date(), 3),
    (15, "Łukasz", "Wieczorek", 40, "IT", 11500.0, datetime.strptime("2016-03-01", "%Y-%m-%d").date(), 4)
]

#Utworzenie DataFrame na podstawie schematu
df_employees = spark.createDataFrame(data, schema)

#Zapis do pliku Parquet w folderze roboczym
output_path = "pracownicy.parquet"
df_employees.write.mode("overwrite").parquet(output_path)

print(f"Plik Parquet został zapisany w: {output_path}")

#Zamknięcie sesji
spark.stop()