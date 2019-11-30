
#setup spark
from pyspark.sql import SparkSession
import databricks.koalas as ks
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from neo4j import GraphDatabase


#Database Handler: 
class ORM(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def create_paper(self, id, title, year, domain, author):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_paper, id, title, year, domain, author)
#             print(greeting)
        return True
    
    def add_relationship(self, source_id, destination_id):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._add_relationship, source_id, destination_id)
        return True

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_paper(tx, id, title, year, domain, author):
        result = tx.run("CREATE (p:Paper {id:{id}, title: {title}, year: {year}, domain: {domain}, author:{author}}) RETURN p", id=id, title=title, year=year, domain=domain, author=author)
        return result.single()[0]
    
    @staticmethod
    def _add_relationship(tx, source, destination):
        result = tx.run("MATCH (src:Paper {id: {source}}), (dest:Paper {id: {destination}}) MERGE (src)-[r:DIRECTED]->(dest)", source=source, destination=destination)
    

database = ORM('bolt://104.248.28.131:7687','neo4j','abdullah-mateen')
papers_path = "/Users/abdullah/Desktop/dataset/hw4/v.csv"
citation_path = "/Users/abdullah/Desktop/dataset/hw4/e.csv"

large_paper_title_path = "/Users/abdullah/Desktop/citation-data/large/paper_title.tsv"
large_paper_year_path = "/Users/abdullah/Desktop/citation-data/large/paper_year.tsv"
large_citation_path = "/Users/abdullah/Desktop/citation-data/large/ref.tsv"


sparkSession = SparkSession.builder.appName("PySpark").master("local").getOrCreate()
sc = sparkSession.sparkContext
sc.setLogLevel("WARN")
papers = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(papers_path)
citations = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(citation_path)

# papers.columns = ['title', 'year', 'domain', 'author']
papers = papers.withColumnRenamed('_c0','id').withColumnRenamed('_c1','title').withColumnRenamed('_c2','year').withColumnRenamed('_c3','domain').withColumnRenamed('_c4','author')
citations = citations.withColumnRenamed('_c0','source_paper_id').withColumnRenamed('_c1','destination_paper_id')

papers.show()
citations.show()

#query result verification using dataframe

citations.createOrReplaceTempView("paper_citation")
papers.createOrReplaceTempView("papers")

# sparkSession.sql("SELECT * FROM `paper_citation` WHERE `year` = '2001' AND source_paper_id = 1830  LIMIT 10").show()
# sparkSession.sql("SELECT ID FROM `papers` WHERE  `title` = 'ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging'").show()

# sparkSession.sql("SELECT * FROM `papers` WHERE `id` IN (SELECT `source_paper_id` FROM `paper_citation` WHERE `destination_paper_id` = 1830) AND year = 2001").show()


sparkSession.sql("SELECT * FROM `paper_citation` WHERE `source_paper_id` = `destination_paper_id`").show()


# sparkSession.sql("SELECT * FROM `papers` WHERE id = 1830").show()


papers_title = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load(large_paper_title_path)
papers_year = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load(large_paper_year_path)
citations = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load(large_citation_path)


papers_title.createOrReplaceTempView("papers_title")
papers_year.createOrReplaceTempView("papers_year")

# sparkSession.sql("SELECT * FROM `papers_year`").show()

large_data = sparkSession.sql("SELECT papers_title.id, papers_title.title, papers_year.year FROM `papers_title` LEFT JOIN papers_year ON `papers_title`.id = `papers_year`.id WHERE year IS NOT NULL")
large_data.show()

#insert papers 
import time

for row in large_data.rdd.collect():
    database.create_paper(row['id'], row['title'], row['year'], 0, 0)
    time.sleep(0.05)

#create relationships
for row in citations.rdd.collect():
    database.add_relationship(row['source_paper_id'], row['destination_paper_id'])

# database.add_relationship("88", "81")
    


database.create_paper(15, "Abdullah", 1992, "it doesn't make sense", "Adil Khan")


#Queries: 
# MATCH (p:Paper {year: '2001'})-[:DIRECTED*7]->(t:Paper {title: "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"}) RETURN p


# CALL algo.pageRank.stream('Paper', 'Paper {title: "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"}', {iterations:20, dampingFactor:0.85})
# YIELD nodeId, score

# RETURN algo.asNode(nodeId).year AS page,score
# ORDER BY score DESC


# 3. Return the most influential papers in the citation graph.
# CALL algo.pageRank.stream('Paper', 'Paper', {iterations:10, dampingFactor:0.85})
# YIELD nodeId, score

# RETURN algo.asNode(nodeId).id AS page,score
# ORDER BY score DESC


# MATCH (p: Paper) RETURN p.community, count(p) as count
# ORDER BY count DESC
# LIMIT 5

