# import pyspark
# from pyspark.sql import SparkSession # Import SparkSession

# # Initialize a Spark session
# spark = (
#     SparkSession.builder
#     #.remote('sc://localhost:15002')
#     .appName("My App")
#     .getOrCreate()
# )
# dflt=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/linktarget/")
# dfp=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/page/")
# dfpl=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/pagelinks/")
# dfr=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/redirect/")
# dflt.show()
# dfp.show()
# dfpl.show()
# dfr.show()

# from pyspark.sql import (
#     SparkSession,
#     functions as F,
#     types as T
# )

# import pyspark
# from pyspark import StorageLevel
# from datetime import datetime

# # Registering DataFrames as temporary views
# dfp.createOrReplaceTempView("page")
# dfr.createOrReplaceTempView("redirect")
# dfpl.createOrReplaceTempView("pagelinks")
# dflt.createOrReplaceTempView("linktarget")

# def mutual_links():
#     # Step 1: Inner join linktarget with page based on lt_title and page_title
#     lt_page_df = spark.table("linktarget") \
#         .join(
#             spark.table("page"),
#             F.col("lt_title") == F.col("page_title"),
#             "inner"
#         ) \
#         .select(
#             F.col("lt_id"),
#             F.col("page_id").alias("lt_page_id")
#         )

#     lt_page_df.createOrReplaceTempView("lt_page")  
#     lt_page_df.show()

#     # Step 2: Left join the result of Step 1 with redirect based on rd_from and lt_page_id
#     lt_redirect_df = lt_page_df \
#         .join(
#             spark.table("redirect"),
#             F.col("lt_page_id") == F.col("rd_from"),
#             "left"
#         ) \
#         .select(
#             F.col("lt_id"),
#             F.col("lt_page_id"),
#             F.col("rd_title")
#         )

#     lt_redirect_df.createOrReplaceTempView("lt_redirect")
#     lt_redirect_df.show()

#     # Step 3: Left join the result of Step 2 with page based on rd_title and page_title
#     lt_resolved_df = lt_redirect_df \
#         .join(
#             spark.table("page"),
#             F.col("rd_title") == F.col("page_title"),
#             "left"
#         ) \
#         .select(
#             F.col("lt_id"),
#             F.coalesce(F.col("page_id"), F.col("lt_page_id")).alias("resolved_page_id")
#         )

#     lt_resolved_df.createOrReplaceTempView("lt_resolved")
#     lt_resolved_df.show()

#     # Step 4: Inner join the result of Step 3 with pagelinks based on pl_target_id and lt_id
#     # Alias the pagelinks table for clear differentiation
#     pagelinks_alias_1 = spark.table("pagelinks").alias("pl1")
#     pagelinks_alias_2 = spark.table("pagelinks").alias("pl2")

#     # Join to find reciprocal links
#     reciprocal_links_df = lt_resolved_df \
#         .join(
#             pagelinks_alias_1,
#             F.col("resolved_page_id") == F.col("pl1.pl_from"),
#             "inner"
#         ) \
#         .join(
#             pagelinks_alias_2,
#             (F.col("lt_id") == F.col("pl2.pl_target_id")) & (F.col("pl2.pl_from") == F.col("resolved_page_id")),
#             "inner"
#         ) \
#         .select(
#             F.col("resolved_page_id").alias("page_a"),
#             F.col("lt_id").alias("page_b")
#         ) \
#         .distinct()  # Remove duplicates

#     reciprocal_links_df.createOrReplaceTempView("reciprocal_links")
#     reciprocal_links_df.show()

#     return reciprocal_links_df

# reciprocal_links_df = mutual_links()

# import os
# import time

# def connected_components(reciprocal_links_df, checkpoint_path="s3://sharadhakasi/wikipedia_component/checkpoint/"):
#     try:
#         print("Starting the connected_components function...")

#         # Step 1: Create bidirectional edges
#         print("Step 1: Creating bidirectional edges...")
#         edges = reciprocal_links_df.selectExpr("page_a as src", "page_b as dst").union(
#             reciprocal_links_df.selectExpr("page_b as src", "page_a as dst")
#         )
#         edges.persist()
#         print(f"Step 1: edges row count: {edges.count()}")

#         # Step 2: Initialize vertices with their IDs as component IDs
#         print("Step 2: Initializing vertices...")
#         vertices = edges.select(F.col("src").alias("vertex")).union(
#             edges.select(F.col("dst").alias("vertex"))
#         ).distinct().withColumn("component", F.col("vertex"))
#         print(f"Step 2: vertices row count: {vertices.count()}")

#         # Step 3: Iterative connected components algorithm
#         print("Step 3: Running iterative connected components algorithm...")
#         iteration = 0
#         changed_vertices = float("inf")  # Arbitrary large number to start
#         start_time = time.time()

#         while changed_vertices > 0:
#             print(f"Iteration {iteration}: Starting...")
#             iteration_start_time = time.time()

#             # Alias DataFrames for disambiguation
#             edges_alias = edges.alias("edges")
#             vertices_alias = vertices.alias("vertices")

#             # Propagate the smallest component ID
#             propagated_components = edges_alias.join(
#                 vertices_alias,
#                 edges_alias.src == vertices_alias.vertex,
#                 "inner"
#             ).select(
#                 F.col("edges.dst").alias("vertex"),
#                 F.col("vertices.component").alias("component")
#             ).union(vertices).groupBy("vertex").agg(F.min("component").alias("component"))

#             # Alias DataFrames to disambiguate during change detection
#             propagated_alias = propagated_components.alias("propagated")
#             original_alias = vertices.alias("original")

#             # Count the number of changed vertices
#             changed_vertices = propagated_alias.join(
#                 original_alias,
#                 "vertex"
#             ).filter(
#                 F.col("propagated.component") != F.col("original.component")
#             ).count()

#             # Update vertices with the new components
#             vertices.unpersist()  # Unpersist previous iteration data to free memory
#             vertices = propagated_components.persist()  # Persist the new vertices

#             elapsed_time = time.time() - iteration_start_time
#             print(f"Iteration {iteration}: Changed nodes = {changed_vertices}, Elapsed time = {elapsed_time:.2f}s")

#             # Save checkpoint every 5 iterations
#             if iteration > 0 and iteration % 5 == 0:
#                 checkpoint_dir = os.path.join(checkpoint_path, f"iteration_{iteration}")
#                 vertices.write.mode("overwrite").parquet(checkpoint_dir)
#                 print(f"Checkpoint saved to: {checkpoint_dir}")

#                 # Reload checkpoint
#                 vertices = spark.read.parquet(checkpoint_dir).persist()
#                 print(f"Checkpoint reloaded from: {checkpoint_dir}")

#             iteration += 1

#         print("Convergence reached.")
#         total_time = time.time() - start_time
#         print(f"Total iterations: {iteration}, Total elapsed time: {total_time:.2f}s")

#         # Step 4: Compute component stats
#         print("Step 4: Computing component statistics...")
#         component_stats = vertices.groupBy("component").agg(F.count("*").alias("size")).persist()

#         print("Top 10 components by size:")
#         component_stats.orderBy(F.desc("size")).show(10, truncate=False)

#         # Step 5: Extract and display details of the largest component
#         print("Step 5: Extracting and displaying the largest component...")
#         largest_component_id = component_stats.orderBy(F.desc("size")).first()["component"]
#         largest_component_vertices = vertices.filter(vertices.component == largest_component_id)

#         print(f"Vertices and Components for the Largest Component (ID: {largest_component_id}):")
#         largest_component_vertices.orderBy("vertex").show(20, truncate=False)

#         # Return results
#         return {
#             "vertices": vertices,
#             "component_stats": component_stats,
#             "largest_component_vertices": largest_component_vertices
#         }

#     except Exception as e:
#         print(f"An error occurred: {str(e)}")
#         raise

# # Run the connected_components function
# result = connected_components(reciprocal_links_df)

# # Access results
# vertices = result["vertices"]
# component_stats = result["component_stats"]
# largest_component_vertices = result["largest_component_vertices"]
#####################################################################################

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
    print("Starting mutual_links function...")
    
    # Step 1: Join `linktarget` with `page`
    print("Step 1: Joining linktarget with page...")
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
    print("Step 2: Joining with redirect...")
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
    print("Step 3: Resolving redirects...")
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
    print("Step 4: Finding reciprocal links...")
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
    print("Starting connected_components function...")
    
    # Step 1: Create bidirectional edges
    print("Step 1: Creating bidirectional edges...")
    edges = (
        reciprocal_links_df
        .selectExpr("page_a as src", "page_b as dst")
        .union(reciprocal_links_df.selectExpr("page_b as src", "page_a as dst"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"Step 1: Edges row count: {edges.count()}")

    # Step 2: Initialize vertices
    print("Step 2: Initializing vertices...")
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
        print(f"Iteration {iteration}: Starting...")
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
        if iteration > 0 and iteration % 5 == 0:
            checkpoint_dir = os.path.join(checkpoint_path, f"iteration_{iteration}")
            vertices.write.mode("overwrite").parquet(checkpoint_dir)
            vertices = spark.read.parquet(checkpoint_dir).persist(StorageLevel.MEMORY_AND_DISK)
            print(f"Checkpoint saved and reloaded from: {checkpoint_dir}")

        iteration += 1

    print("Convergence reached.")
    total_time = time.time() - start_time
    print(f"Total iterations: {iteration}, Total elapsed time: {total_time:.2f}s")

    # Step 4: Compute component statistics
    print("Step 4: Computing component statistics...")
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
