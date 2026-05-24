import os
import sys
from pyspark.sql import SparkSession

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("SparkSQL_Parquet") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

#Ścieżka do przygotowanego pliku Parquet
file_path = "pracownicy.parquet"

#Wczytanie pliku Parquet do obiektu DataFrame
df_parquet = spark.read.parquet(file_path)

#Wyświetlenie pierwszych 5 wierszy oraz struktury danych
print("Dane z pliku Parquet (Pierwsze 5 wierszy):")
df_parquet.show(5)

print("Pełny schemat danych Parquet:")
df_parquet.printSchema()

print(f"Łączna liczba wczytanych rekordów: {df_parquet.count()}")

#Zamykanie sesji Sparka
spark.stop()