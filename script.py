from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from graphframes import *
import os
from pyspark.sql.functions import explode


#
# os.environ["PYSPARK_SUBMIT_ARGS"] \
#     = "--packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 --repositories https://dl.bintray.com/spark-packages/maven/ pyspark-shell"

def create_graph():
    vert = spark.read.csv("v.txt", sep="\t").toDF("id", "title", "year", "journal", "authors")
    edges = spark.read.csv("e.txt", sep="\t").toDF("src", "dst", "ones")
    return GraphFrame(vert, edges)

def create_large_graph():
        # read large dataset and merge it
    # create dataframe from 2 files  and then filter from nulls
    v1 = spark.read.option("header", "true") \
        .csv("paper_year.tsv", sep="\t", header=True).toDF("id", "year")
    v2 = spark.read.option("header", "true") \
        .csv("paper_title.tsv", sep="\t", header=True).toDF("id", "title")

    ver_large = v1.join(v2, on=['id'], how='full')
    ver_large.where(ver_large.id.isNotNull()).sort("id")

    ed = spark.read.option("delimiter", "\t") \
        .csv("ref.tsv", sep="\t").toDF("src", "dst")
    ed.sort("src")

    # create graph frame for large dataset
    return GraphFrame(ver_large, ed)

if __name__ == '__main__':
    sqlContext = SQLContext(spark.sparkContext)
    spark = SparkSession \
        .builder \
        .getOrCreate()

    g_small = create_graph()
    g_large = create_large_graph()
    # query for small graphframe
    x = g_small.vertices.filter("title='ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging'")
    short_path_small = g_small.shortestPaths(landmarks=[x.take(1)[0]["id"]]).filter("year=2001")
    short_path_small.select("id", explode("distances")).show()

    # query for large graphframe
    id_ml = g_large.vertices.filter("title='Machine Learning'")
    short_large = g_large.shortestPaths(landmarks=[id_ml.take(1)[0]["id"]])
    short_large.select("id", explode("distances")).sort("value",ascending= False).show()

    # Finding year with biggest amount of traced papers to "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"
    short_path_small.select("id", "year", explode("distances")).groupby("year").count().sort("count", ascending=False).show(1)

    # Finding year with biggest amount of traced papers to "Machine learning"
    short_large.select("id", "year", explode("distances")).groupby("year").count().sort("count", ascending=False).show(1)


    # labelpropagation
    result = g_small.labelPropagation(maxIter=5)
    result2 = g_large.labelPropagation(maxIter=20)
    result.groupby("label").count().sort("count",ascending=False).show(5)
    result2.groupby("label").count().sort("count",ascending=False).show(5)

    # pagerank
    PRank_small = g_small.pageRank(resetProbability=0.15, tol=0.01)
    PRank_small.vertices.select("id", "pagerank").sort("pagerank",ascending=False).show()
    PRank_large = g_large.pageRank(resetProbability=0.15, tol=0.01)
    PRank_large.vertices.select("id", "pagerank").sort("pagerank",ascending=False).show()
