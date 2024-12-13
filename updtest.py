import unittest
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
import os
import time

class TestMutualLinksAndComponents(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("TestMutualLinksAndComponents")
            .getOrCreate()
        )

        # Load test data from local machine (or replace with the synthetic test data from S3 when required)
        cls.dflt = cls.spark.read.json("C:/Users/Sharadha Kasi/Downloads/synthetic_testdata/linktarget.jsonl")
        cls.dfp = cls.spark.read.json("C:/Users/Sharadha Kasi/Downloads/synthetic_testdata/page.jsonl")
        cls.dfpl = cls.spark.read.json("C:/Users/Sharadha Kasi/Downloads/synthetic_testdata/pagelinks.jsonl")
        cls.dfr = cls.spark.read.json("C:/Users/Sharadha Kasi/Downloads/synthetic_testdata/redirect.jsonl")


        # Register DataFrames as temporary views
        cls.dfp.createOrReplaceTempView("page")
        cls.dfr.createOrReplaceTempView("redirect")
        cls.dfpl.createOrReplaceTempView("pagelinks")
        cls.dflt.createOrReplaceTempView("linktarget")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def mutual_links(self):
        # Step 1 to Step 9 from mutual_links function
        filtered_pages_df = (
            self.spark.sql("SELECT * FROM page WHERE page_namespace = 0")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        filtered_pages_df.createOrReplaceTempView("filtered_pages")

        page_redirects_df = (
            filtered_pages_df
            .join(self.dfr, filtered_pages_df.page_id == self.dfr.rd_from, "left")
            .select(
                F.col("page_id"),
                F.col("page_title"),
                F.col("page_namespace"),
                F.col("page_is_redirect"),
                F.col("rd_title")
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        filtered_pages_df.unpersist()
        page_redirects_df.createOrReplaceTempView("page_redirects")

        link_targets_df = (
            page_redirects_df
            .join(
                self.dflt,
                (page_redirects_df.page_title == self.dflt.lt_title) &
                (page_redirects_df.page_namespace == self.dflt.lt_namespace),
                "inner"
            )
            .select(
                F.col("lt_id"),
                F.col("page_id"),
                F.col("page_title"),
                F.col("page_namespace"),
                F.col("page_is_redirect"),
                F.col("rd_title")
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        page_redirects_df.unpersist()
        link_targets_df.createOrReplaceTempView("link_targets")

        updated_link_targets_df = (
            link_targets_df
            .withColumn(
                "lt_title_updated",
                F.when(F.col("page_is_redirect") == False, F.col("page_title"))
                .otherwise(F.col("rd_title"))
            )
            .select("lt_id", "lt_title_updated")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        link_targets_df.unpersist()
        updated_link_targets_df.createOrReplaceTempView("updated_link_targets")

        resolved_links_df = (
            updated_link_targets_df.alias("updated_links")
            .join(
                self.spark.table("filtered_pages").alias("filtered_pages"),
                (
                    F.col("updated_links.lt_title_updated") == F.col("filtered_pages.page_title")
                ) & (F.col("filtered_pages.page_namespace") == 0),
                "left"
            )
            .select(
                F.col("updated_links.lt_id"),
                F.col("updated_links.lt_title_updated"),
                F.col("filtered_pages.page_id")
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        updated_link_targets_df.unpersist()
        resolved_links_df.createOrReplaceTempView("resolved_links")

        page_links_df = (
            resolved_links_df.alias("resolved_links")
            .join(
                self.dfpl.alias("pagelinks"),
                F.col("resolved_links.lt_id") == F.col("pagelinks.pl_target_id"),
                "inner"
            )
            .select(
                F.col("pagelinks.pl_from").alias("page_a"),
                F.col("resolved_links.page_id").alias("page_b")
            )
            .distinct()
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        resolved_links_df.unpersist()
        page_links_df.createOrReplaceTempView("page_links")

        sorted_pairs_df = (
            page_links_df
            .withColumn(
                "sorted_pair",
                F.array_sort(F.array(F.col("page_a"), F.col("page_b")))
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        page_links_df.unpersist()
        sorted_pairs_df.createOrReplaceTempView("sorted_pairs")

        pair_frequencies_df = (
            sorted_pairs_df
            .groupBy("sorted_pair")
            .agg(F.count("*").alias("frequency"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        sorted_pairs_df.unpersist()
        pair_frequencies_df.createOrReplaceTempView("pair_frequencies")

        mutual_links_df = (
            pair_frequencies_df
            .filter(F.col("frequency") > 1)
            .withColumn("page_a", F.col("sorted_pair")[0])
            .withColumn("page_b", F.col("sorted_pair")[1])
            .select("page_a", "page_b")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        pair_frequencies_df.unpersist()
        mutual_links_df.createOrReplaceTempView("mutual_links")
        return mutual_links_df

    def test_mutual_links(self):
        """Test mutual_links function."""
        mutual_links_df = self.mutual_links()
        actual_links = frozenset(tuple(row) for row in mutual_links_df.collect())

        # Example expected result for testing purpose
        expected_links = frozenset({
            (1, 8), (3, 9), (4, 8), (6, 9), (7, 10)
        })
        self.assertEqual(
            actual_links, expected_links,
            f"Mutual links mismatch. Expected: {expected_links}, Got: {actual_links}"
        )

    def test_connected_components(self):
        """Test connected_components function."""
        mutual_links_df = self.mutual_links()
        edges = (
            mutual_links_df
            .selectExpr("page_a as src", "page_b as dst")
            .union(mutual_links_df.selectExpr("page_b as src", "page_a as dst"))
        )
        vertices = (
            edges
            .select(F.col("src").alias("vertex"))
            .union(edges.select(F.col("dst").alias("vertex")))
            .distinct()
            .withColumn("component", F.col("vertex"))
        )

        # Expected result for testing purpose
        expected_components = frozenset({
            frozenset({1, 2, 8}),  # Component with members {1, 2, 8}
            frozenset({3, 4, 9}),  # Component with members {3, 4, 9}
            frozenset({5, 6}),     # Component with members {5, 6}
            frozenset({7, 10})     # Component with members {7, 10}
        })

        actual_components = frozenset(
            frozenset(row.members)
            for row in vertices.groupBy("component")
            .agg(F.collect_list("vertex").alias("members"))
            .collect()
        )
        self.assertEqual(
            actual_components, expected_components,
            f"Components mismatch. Expected: {expected_components}, Got: {actual_components}"
        )

if __name__ == "__main__":
    unittest.main()
