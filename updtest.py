import unittest
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
import os

class TestMutualLinksAndComponents(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("TestMutualLinksAndComponents")
            .getOrCreate()
        )

        # Load test data from JSON files (replace paths if necessary)
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
        # Debugging and improved mutual_links logic
        print("Debugging mutual_links:")

        # Step 1: Filter pages with namespace 0
        filtered_pages_df = (
            self.spark.sql("SELECT * FROM page WHERE page_namespace = 0")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        filtered_pages_df.createOrReplaceTempView("filtered_pages")
        print("Filtered Pages:")
        filtered_pages_df.show()

        # Step 2: Join filtered pages with redirects
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
        print("Page Redirects:")
        page_redirects_df.show()

        # Step 3: Join with link targets
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
        print("Link Targets:")
        link_targets_df.show()

        # Step 4: Resolve redirects
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
        print("Updated Link Targets:")
        updated_link_targets_df.show()

        # Step 5: Join updated link targets back to pages
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
        print("Resolved Links:")
        resolved_links_df.show()

        # Step 6: Join with page links
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
        print("Page Links:")
        page_links_df.show()

        # Step 9: Filter for mutual links
        mutual_links_df = (
            page_links_df
            .filter(F.col("page_a") < F.col("page_b"))  # Ensure ordered pairs
            .distinct()
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        mutual_links_df.createOrReplaceTempView("mutual_links")
        print("Mutual Links:")
        mutual_links_df.show()
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

if __name__ == "__main__":
    unittest.main()
