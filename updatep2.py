import pyspark
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel
import os
import time

# Initialize a Spark session
spark = (
    SparkSession.builder
    .appName("Optimized Mutual Links and Connected Components")
    .getOrCreate()
)

# Load data from S3
dflt = spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/linktarget/")
dfp = spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/page/")
dfpl = spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/pagelinks/")
dfr = spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/redirect/")

# Display data for verification
print("Displaying linktarget:")
dflt.show()
print("Displaying page:")
dfp.show()
print("Displaying pagelinks:")
dfpl.show()
print("Displaying redirect:")
dfr.show()

# Register DataFrames as temporary views
dfp.createOrReplaceTempView("page")
dfr.createOrReplaceTempView("redirect")
dfpl.createOrReplaceTempView("pagelinks")
dflt.createOrReplaceTempView("linktarget")

# Mutual Links Function
def mutual_links():
    print("Starting mutual_links function....")

    # Step 1: Filter Pages with Namespace 0
    print("Step 1: Filtering pages with namespace 0....")
    filtered_pages_df = (
        spark.sql("SELECT * FROM page WHERE page_namespace = 0")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    filtered_pages_df.createOrReplaceTempView("filtered_pages")
    print("Step 1: Result:")
    filtered_pages_df.show()

    # Step 2: Join Filtered Pages with Redirects
    print("Step 2: Joining filtered pages with redirects....")
    page_redirects_df = (
        filtered_pages_df
        .join(dfr, filtered_pages_df.page_id == dfr.rd_from, "left")
        .select(
            F.col("page_id"),
            F.col("page_title"),
            F.col("page_namespace"),  # Retain namespace
            F.col("page_is_redirect"),
            F.col("rd_title")
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    filtered_pages_df.unpersist()
    page_redirects_df.createOrReplaceTempView("page_redirects")
    print("Step 2: Result:")
    page_redirects_df.show()

    # Step 3: Join with Link Targets
    print("Step 3: Joining with link targets....")
    link_targets_df = (
        page_redirects_df
        .join(
            dflt,
            (page_redirects_df.page_title == dflt.lt_title) &
            (page_redirects_df.page_namespace == dflt.lt_namespace),
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
    print("Step 3: Result:")
    link_targets_df.show()

    # Step 4: Update Link Target Titles
    print("Step 4: Updating link target titles....")
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
    print("Step 4: Result:")
    updated_link_targets_df.show()

    # Step 5: Join Updated Link Targets Back to Pages
    print("Step 5: Joining updated link targets back to pages....")
    resolved_links_df = (
        updated_link_targets_df.alias("updated_links")
        .join(
            filtered_pages_df.alias("filtered_pages"),
            (
                F.col("updated_links.lt_title_updated") == F.col("filtered_pages.page_title")
            ) & (F.col("filtered_pages.page_namespace") == 0),  # Ensure main namespace
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
    print("Step 5: Result:")
    resolved_links_df.show()

    # Step 6: Join with Page Links
    print("Step 6: Joining with page links....")
    page_links_df = (
        resolved_links_df.alias("resolved_links")
        .join(
            dfpl.alias("pagelinks"),
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
    print("Step 6: Result:")
    page_links_df.show()

    # Step 7: Sort Page Pairs
    print("Step 7: Sorting page pairs....")
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
    print("Step 7: Result:")
    sorted_pairs_df.show()

    # Step 8: Group and Count Pair Frequencies
    print("Step 8: Grouping and counting pair frequencies....")
    pair_frequencies_df = (
        sorted_pairs_df
        .groupBy("sorted_pair")
        .agg(F.count("*").alias("frequency"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    sorted_pairs_df.unpersist()
    pair_frequencies_df.createOrReplaceTempView("pair_frequencies")
    print("Step 8: Result:")
    pair_frequencies_df.show()

    # Step 9: Filter for Mutual Links
    print("Step 9: Filtering for mutual links....")
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
    print("Step 9: Result:")
    mutual_links_df.show()
    
    print(f"Total mutual pairs: {mutual_links_df.count()}")
    return mutual_links_df
    

# Connected Components Function
def connected_components(reciprocal_links_df, checkpoint_path="s3://sharadhakasi/wikipedia_comp/checkpoint/"):
    print("Starting connected_components function....")
    
    # Step 1: Create bidirectional edges
    print("Step 1: Creating bidirectional edges....")
    edges = (
        reciprocal_links_df
        .selectExpr("page_a as page_a", "page_b as page_b")
        .union(reciprocal_links_df.selectExpr("page_b as page_a", "page_a as page_b"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"Step 1: Edges row count: {edges.count()}")

    # Step 2: Initialize vertices
    print("Step 2: Initializing vertices....")
    vertices = (
        edges
        .select(F.col("page_a").alias("vertex"))
        .union(edges.select(F.col("page_b").alias("vertex")))
        .distinct()
        .withColumn("component", F.col("vertex"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"Step 2: Vertices row count: {vertices.count()}")

    # Step 3: Iterative connected components algorithm
    iteration = 0
    changed_vertices = float("inf")
    start_time = time.time()

    while changed_vertices > 0:
        print(f"Iteration {iteration}: Starting....")
        iteration_start_time = time.time()

        # Alias the edges and vertices DataFrames for disambiguation
        edges_alias = edges.alias("edges")
        vertices_alias = vertices.alias("vertices")

        # Propagate the smallest component ID
        propagated_components = (
            edges_alias
            .join(
                vertices_alias,
                edges_alias.page_a == vertices_alias.vertex,
                "inner"
            )
            .select(
                F.col("edges.page_b").alias("vertex"),
                F.col("vertices.component").alias("component")
            )
            .union(vertices)
            .groupBy("vertex")
            .agg(F.min("component").alias("component"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Calculate changed vertices
        changed_vertices = (
            propagated_components.alias("propagated")
            .join(vertices.alias("original"), "vertex")
            .filter(
                F.col("propagated.component") != F.col("original.component")
            )
            .count()
        )

        # Update vertices for the next iteration
        vertices.unpersist()
        vertices = propagated_components

        elapsed_time = time.time() - iteration_start_time
        print(f"Iteration {iteration}: Changed nodes = {changed_vertices}, Elapsed time = {elapsed_time:.2f}s")

        # Save checkpoint every 5 iterations
        if iteration > 0 and iteration % 3 == 0:
            checkpoint_dir = os.path.join(checkpoint_path, f"iteration_{iteration}")
            vertices.write.mode("overwrite").parquet(checkpoint_dir)
            vertices = spark.read.parquet(checkpoint_dir).persist(StorageLevel.MEMORY_AND_DISK)
            print(f"Checkpoint saved and reloaded from: {checkpoint_dir}")

        iteration += 1

    print("Convergence reached.")
    total_time = time.time() - start_time
    print(f"Total iterations: {iteration}, Total elapsed time: {total_time:.2f}s")

    # Step 4: Compute component statistics
    print("Step 4: Computing component statistics....")
    component_stats = vertices.groupBy("component").agg(F.count("*").alias("size")).persist(StorageLevel.MEMORY_AND_DISK)
    component_stats.orderBy(F.desc("size")).show(10, truncate=False)

    # Extract details of the largest component
    largest_component_id = component_stats.orderBy(F.desc("size")).first()["component"]
    largest_component_vertices = vertices.filter(F.col("component") == largest_component_id)
    largest_component_vertices.show(20, truncate=False)

    return {
        "vertices": vertices,
        "component_stats": component_stats,
        "largest_component_vertices": largest_component_vertices
    }

# Run Mutual Links and Connected Components
reciprocal_links_df = mutual_links()
result = connected_components(reciprocal_links_df)
