import unittest
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
import time
import os

# Define the expected results as frozen sets for testing
EXPECTED_LINKS = frozenset({
    (1, 8), (3, 9), (4, 8), (6, 9), (7, 10)
})

EXPECTED_COMPONENTS = frozenset({
    frozenset({1, 2, 8}),  # Component with members {1, 2, 8}
    frozenset({3, 4, 9}),  # Component with members {3, 4, 9}
    frozenset({5, 6}),     # Component with members {5, 6}
    frozenset({7, 10})     # Component with members {7, 10}
})


class TestMutualLinksAndComponents(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the Spark session."""
        cls.spark = (
            SparkSession.builder
            .master("local[*]")  # Ensure local mode is set correctly
            .appName("TestMutualLinksAndComponents")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", "AKIAQ6FQSQFDM2IWFSV5")
            .config("spark.hadoop.fs.s3a.secret.key", "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")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.driver.memory", "2g")  # Adjust memory as needed
            .config("spark.executor.memory", "2g")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session."""
        cls.spark.stop()

    def mutual_links(self):
        print("Starting mutual_links function....")
        
        # Step 1: Join `linktarget` with `page`
        print("Step 1: Joining linktarget with page....")
        lt_page_df = (
            self.spark.table("linktarget")
            .join(self.spark.table("page"), F.col("lt_title") == F.col("page_title"), "inner")
            .select(F.col("lt_id"), F.col("page_id").alias("lt_page_id"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        lt_page_df.createOrReplaceTempView("lt_page")

        # Step 2: Join with `redirect`
        lt_redirect_df = (
            lt_page_df
            .join(self.spark.table("redirect"), F.col("lt_page_id") == F.col("rd_from"), "left")
            .select(F.col("lt_id"), F.col("lt_page_id"), F.col("rd_title"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        lt_page_df.unpersist()
        lt_redirect_df.createOrReplaceTempView("lt_redirect")

        # Step 3: Resolve redirects by joining with `page`
        lt_resolved_df = (
            lt_redirect_df
            .join(self.spark.table("page"), F.col("rd_title") == F.col("page_title"), "left")
            .select(F.col("lt_id"), F.coalesce(F.col("page_id"), F.col("lt_page_id")).alias("resolved_page_id"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        lt_redirect_df.unpersist()
        lt_resolved_df.createOrReplaceTempView("lt_resolved")

        # Step 4: Identify reciprocal links
        reciprocal_links_df = (
            lt_resolved_df
            .join(
                self.spark.table("pagelinks").alias("pl1"), F.col("resolved_page_id") == F.col("pl1.pl_from"), "inner"
            )
            .join(
                self.spark.table("pagelinks").alias("pl2"),
                (F.col("lt_id") == F.col("pl2.pl_target_id")) & (F.col("pl2.pl_from") == F.col("resolved_page_id")),
                "inner"
            )
            .select(F.col("resolved_page_id").alias("page_a"), F.col("lt_id").alias("page_b"))
            .distinct()
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        lt_resolved_df.unpersist()

        return reciprocal_links_df

    def connected_components(self, reciprocal_links_df, checkpoint_path="s3://sharadhakasi/wikipedia_comp/checkpoint/"):
        print("Starting connected_components function....")
        
        # Step 1: Create bidirectional edges
        edges = (
            reciprocal_links_df
            .selectExpr("page_a as src", "page_b as dst")
            .union(reciprocal_links_df.selectExpr("page_b as src", "page_a as dst"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Step 2: Initialize vertices
        vertices = (
            edges
            .select(F.col("src").alias("vertex"))
            .union(edges.select(F.col("dst").alias("vertex")))
            .distinct()
            .withColumn("component", F.col("vertex"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Step 3: Iterative connected components algorithm
        iteration = 0
        changed_vertices = float("inf")
        start_time = time.time()

        while changed_vertices > 0:
            propagated_components = (
                edges.join(vertices, edges.src == vertices.vertex, "inner")
                .select(F.col("dst").alias("vertex"), F.col("component"))
                .union(vertices)
                .groupBy("vertex")
                .agg(F.min("component").alias("component"))
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            changed_vertices = (
                propagated_components.alias("propagated")
                .join(vertices.alias("original"), "vertex")
                .filter(F.col("propagated.component") != F.col("original.component"))
                .count()
            )

            vertices.unpersist()
            vertices = propagated_components

            iteration += 1

        # Step 4: Compute component statistics
        component_stats = vertices.groupBy("component").agg(F.count("*").alias("size")).persist(StorageLevel.MEMORY_AND_DISK)

        # Extract details of the largest component
        largest_component_id = component_stats.orderBy(F.desc("size")).first()["component"]
        largest_component_vertices = vertices.filter(F.col("component") == largest_component_id)

        return {
            "vertices": vertices,
            "component_stats": component_stats,
            "largest_component_vertices": largest_component_vertices
        }

    def test_mutual_links_and_connected_components(self):
        """Test mutual links and connected components functionality."""
        # Load test data from S3
        linktarget = self.spark.read.json("s3a://sharadhakasi/synthetic_testdata/linktarget.jsonl")
        page = self.spark.read.json("s3a://sharadhakasi/synthetic_testdata/page.jsonl")
        pagelinks = self.spark.read.json("s3a://sharadhakasi/synthetic_testdata/pagelinks.jsonl")
        redirect = self.spark.read.json("s3a://sharadhakasi/synthetic_testdata/redirect.jsonl")

        # Register as temporary views
        linktarget.createOrReplaceTempView("linktarget")
        page.createOrReplaceTempView("page")
        pagelinks.createOrReplaceTempView("pagelinks")
        redirect.createOrReplaceTempView("redirect")

        # Execute mutual_links function
        mutual_links_df = self.mutual_links()

        # Verify mutual links result
        actual_links = frozenset(tuple(row) for row in mutual_links_df.collect())
        self.assertEqual(
            actual_links, EXPECTED_LINKS,
            f"Mutual links mismatch. Expected: {EXPECTED_LINKS}, Got: {actual_links}"
        )

        # Execute connected_components function
        result = self.connected_components(mutual_links_df)

        # Verify connected components
        components = result["vertices"].groupBy("component").agg(
            F.collect_list("vertex").alias("members")
        )
        actual_components = frozenset(
            frozenset(row.members) for row in components.collect()
        )
        self.assertEqual(
            actual_components, EXPECTED_COMPONENTS,
            f"Connected components mismatch. Expected: {EXPECTED_COMPONENTS}, Got: {actual_components}"
        )

# Run the test suite explicitly
if __name__ == "__main__":
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestMutualLinksAndComponents))