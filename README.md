# CS-535_Large-Scale-Data-Analysis-Project-2

## Reflection on Project 1 and Project 2

### Project 1: Mutual Links
Working on **Project 1: Mutual Links** taught me the fundamentals of working with graph-like data structures in real-world datasets, specifically Wikipedia's interconnected graph of pages and links. One of the key lessons was handling complexities like redirects, which required an understanding of how data transformations affect outcomes. I encountered challenges in ensuring mutual link pairs were unique and handling the extensive size of the dataset within memory and time constraints. An interesting discovery was the prevalence of cyclic relationships in Wikipedia data, often driven by redirects or historical edits, which underscored the importance of preprocessing to achieve accuracy in results. Debugging and verifying the results was particularly rewarding, as it emphasized the importance of thorough data validation.

### Project 2: Connected Components
**Project 2: Connected Components** built upon the first project by introducing iterative processing and the concept of connected components within a graph. This required implementing algorithms capable of scaling to large datasets, as well as checkpointing iterations to maintain progress during execution on distributed systems like EMR. The main difficulty was optimizing the connected components algorithm to meet the performance benchmarks while ensuring correctness, especially given the iterative nature of the task and specifically time constraints for optimal performance. I learned about the importance of designing robust test suites with synthetic datasets to validate edge cases, such as isolated nodes or small cliques within the graph. One fascinating aspect of Wikipedia’s dataset was observing how certain topics form large, dense clusters. These insights not only deepened my understanding of graph processing but also highlighted the richness of real-world data.


# Wikipedia Graph Analysis: Mutual Links and Connected Components

## Project Overview
This project involves analyzing Wikipedia's graph structure to identify mutual links between pages and compute connected components within the graph. The implementation is designed to process large datasets on AWS EMR clusters and includes a robust automated test suite with synthetic datasets.

### Key Features
- **Mutual Links Identification**: Determines pairs of pages that link to each other, accounting for redirects and ensuring uniqueness.
- **Connected Components Algorithm**: Identifies connected components in the graph, with intermediate checkpointing and iterative processing.
- **Synthetic Test Suite**: Validates functionality using a set of synthetic datasets with predefined expected results.

## File Descriptions
- **`prj2.py`**: Contains the complete code for both mutual links identification and connected components computation. This file implements all the core functionality required to process the Wikipedia graph dataset.
- **`entrypoint2.py`**: Serves as the entry point for packaging and running the project on an AWS EMR cluster. Use this file to create a zip package with dependencies for deploying the project to EMR.
- **`prj2script.py`**: Script to automate the start of the EMR cluster and execute the analysis. Running this script (`python prj2script.py`) processes the dataset on EMR and writes the output to the specified S3 bucket.
- **Synthetic Data Files**:
  - `page.jsonl`: Synthetic dataset representing the Wikipedia page table.
  - `linktarget.jsonl`: Synthetic dataset representing link targets.
  - `pagelinks.jsonl`: Synthetic dataset representing page links.
  - `redirect.jsonl`: Synthetic dataset representing redirects.
- **`testp2.py`**: Automated test suite for validating the implementation using the synthetic datasets. It uses a local Spark session to ensure correctness against expected outputs.

## Usage Instructions
### Running the Code on EMR
1. Ensure your environment is configured with AWS credentials and necessary permissions.
2. Run the following command to execute the script non-interactively:
   ```bash
   python prj2script.py

### UPDATE:
->  Please run the updated files: u.py is the EMR script, uep2.py is the entrypoint file, updatep2.py has the main code for project 1 and 2
        Run the following command to execute the script non-interactively:
        ```bash
        python u.py

->  Please run the following command to execute the testing part in local machine, which I have updated and placed in the updtest.py file:  
    ```bash
    python -m unittest updtest.py
