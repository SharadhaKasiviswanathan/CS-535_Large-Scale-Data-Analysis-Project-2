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
    
    # Step 1: Join `linktarget` with `page`
    print("Step 1: Joining linktarget with page....")
    lt_page_df = (
        spark.table("linktarget")
        .join(spark.table("page"), F.col("lt_title") == F.col("page_title"), "inner")
        .select(F.col("lt_id"), F.col("page_id").alias("lt_page_id"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    lt_page_df.createOrReplaceTempView("lt_page")
    print("Step 1: Result:")
    lt_page_df.show()

    # Step 2: Join with `redirect`
    print("Step 2: Joining with redirect....")
    lt_redirect_df = (
        lt_page_df
        .join(spark.table("redirect"), F.col("lt_page_id") == F.col("rd_from"), "left")
        .select(F.col("lt_id"), F.col("lt_page_id"), F.col("rd_title"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    lt_page_df.unpersist()
    lt_redirect_df.createOrReplaceTempView("lt_redirect")
    print("Step 2: Result:")
    lt_redirect_df.show()

    # Step 3: Resolve redirects by joining with `page`
    print("Step 3: Resolving redirects....")
    lt_resolved_df = (
        lt_redirect_df
        .join(spark.table("page"), F.col("rd_title") == F.col("page_title"), "left")
        .select(F.col("lt_id"), F.coalesce(F.col("page_id"), F.col("lt_page_id")).alias("resolved_page_id"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    lt_redirect_df.unpersist()
    lt_resolved_df.createOrReplaceTempView("lt_resolved")
    print("Step 3: Result:")
    lt_resolved_df.show()

    # Step 4: Identify reciprocal links
    print("Step 4: Finding reciprocal links....")
    reciprocal_links_df = (
        lt_resolved_df
        .join(
            spark.table("pagelinks").alias("pl1"), F.col("resolved_page_id") == F.col("pl1.pl_from"), "inner"
        )
        .join(
            spark.table("pagelinks").alias("pl2"),
            (F.col("lt_id") == F.col("pl2.pl_target_id")) & (F.col("pl2.pl_from") == F.col("resolved_page_id")),
            "inner"
        )
        .select(F.col("resolved_page_id").alias("page_a"), F.col("lt_id").alias("page_b"))
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    lt_resolved_df.unpersist()
    reciprocal_links_df.createOrReplaceTempView("reciprocal_links")
    print("Step 4: Result:")
    reciprocal_links_df.show()

    return reciprocal_links_df

# Connected Components Function
def connected_components(reciprocal_links_df, checkpoint_path="s3://sharadhakasi/wikipedia_comp/checkpoint/"):
    print("Starting connected_components function....")
    
    # Step 1: Create bidirectional edges
    print("Step 1: Creating bidirectional edges....")
    edges = (
        reciprocal_links_df
        .selectExpr("page_a as src", "page_b as dst")
        .union(reciprocal_links_df.selectExpr("page_b as src", "page_a as dst"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"Step 1: Edges row count: {edges.count()}")

    # Step 2: Initialize vertices
    print("Step 2: Initializing vertices....")
    vertices = (
        edges
        .select(F.col("src").alias("vertex"))
        .union(edges.select(F.col("dst").alias("vertex")))
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
                edges_alias.src == vertices_alias.vertex,
                "inner"
            )
            .select(
                F.col("edges.dst").alias("vertex"),
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
