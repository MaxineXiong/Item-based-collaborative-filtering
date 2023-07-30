# Leveraging Item-based Collaborative Filtering for Movie Recommendations in PySpark

[![GitHub](https://badgen.net/badge/icon/GitHub?icon=github&color=black&label)](https://github.com/MaxineXiong)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Made with Python](https://img.shields.io/static/v1?label=Python&message=>+3.6%2C+<%3D+3.8&color=%233776AB&logo=python&logoColor=white)](https://www.python.org)
[![Apache Spark](https://img.shields.io/static/v1?label=&message=Apache+Spark&color=%23000000&logo=Apache+Spark&logoColor=%23E25A1C)](https://spark.apache.org/)

<br>

## **Project Description**

This project implements item-based collaborative filtering in **[PySpark DataFrames](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html)** and **[PySpark RDD](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)** to recommend movies with similar rating patterns. The goal is to provide personalized movie recommendations by finding movies similar to a given target movie, either based on **cosine similarity scores of movie ratings** or **the number of shared viewers**.

<br>

## **Features**

- **Item-based Collaborative Filtering**: Utilizing **PySpark DataFrames** and **PySpark RDD** to implement item-based collaborative filtering to recommend movies.
- **Cosine Similarity Score**: Calculating cosine similarity scores of movie ratings to find movies with similar ratings.
- **Number of Shared Viewers**: Identifying movies that have the highest number of shared viewers with the target movie to enhance recommendations.
- **Top-N Recommendations**: Recommending the top 10 movies that are most similar to the target movie.

<br>

## **Repository Structure**

This repository consists of the following files:

```
Leveraging Item-based Collaborative Filtering for Movie Recommendations in PySpark
├── Spark Dataframes - Item-based Collaborative Filtering.py
├── Spark RDD - Item-based Collaborative Filtering.ipynb
├── data
│   ├── ml-1m
│   │   └── [movie rating data with 1 million rows]
│   └── ml-100k
│       └── [movie rating data with 100k rows]
├── README.md
├── APACHE LICENSE
└── MIT LICENSE
```

- **Spark Dataframes - Item-based Collaborative Filtering.py**: A Python script employing **PySpark DataFrames** to implement item-based collaborative filtering on a movie dataset with **100k rows**. It finds the top 10 recommended movies based on cosine similarity scores and the top 10 movies with the highest number of shared viewers.
- **Spark RDD - Item-based Collaborative Filtering.ipynb**: An IPython notebook using **PySpark RDD** to implement item-based collaborative filtering on a larger movie dataset with **1 million rows**. It finds the top 10 movie recommendations with the highest similarity scores.
- **data/ml-1m/**: The subfolder contains the movie rating data with 1 million rows used in the IPython notebook **Spark RDD - Item-based Collaborative Filtering.ipynb**.
- **data/ml-100k/**: The subfolder contains the movie rating data with 100k rows used in the Python script **Spark Dataframes - Item-based Collaborative Filtering.py**.
- **README.md**: The main documentation file you are currently reading. It provides an overview of the repository, project description, usage, and other relevant information.
- **APACHE LICENSE**: The Apache license file for the project.
- **MIT LICENSE**: The MIT license file for the project.

<br>

## **Usage**

**Note**: As of July 2023, PySpark may not be fully compatible with Python 3.11. It is highly recommended to use ***Python 3.7*** or ***3.8*** for executing any PySpark-related operations.

To set up the environment for running the Spark code, follow these steps:

1. Open **Anaconda Prompt** and execute the following command to create a conda environment powered by Python 3.8:

  ```
  conda create -n py38 python=3.8 anaconda
  ```

2. Open **Anaconda Navigator**, go to **Environments**, and select the **py38** environment to install the "**pyspark**" package.
3. In the **py38** environment folder (e.g. "C:\Users\\[your-user-name]\anaconda3\envs\py38"), create a copy of **python.exe** and rename it to “**python3.exe”**.
4. Open the **Jupyter Notebook** application from your **Anaconda Navigator**. The notebooks you open should now run on Python 3.8. You can check the Python version by executing `!python --version` in a notebook code cell.
5. Download this repository and run either **Spark Dataframes - Item-based Collaborative Filtering.py** or **Spark RDD - Item-based Collaborative Filtering.ipynb**.

### **Spark Dataframes - Item-based Collaborative Filtering.py**

To run the **Spark Dataframes - Item-based Collaborative Filtering.py** script:

1. Open **Anaconda Prompt**.
2. Execute the following command to switch to the **py38** environment powered by Python 3.8:

```
conda activate py38
```

3. Navigate to the project folder using the `cd` command.
4. Run the following command (**Note**: Replace `[movie ID entry]` with the movie ID for the target movie for which we want to find similar movies. It must be provided as a command-line argument):

  ```
  spark-submit "Spark Dataframes - Item-based Collaborative Filtering.py" [movie ID entry]
  ```

5. The output information about the top 10 movie recommendations based on cosine similarity scores and the top 10 movies with the highest number of shared viewers will be displayed on Anaconda Prompt, similar to what the snapshot below shows:

  ![movie-recommendations](https://github.com/MaxineXiong/Item-based-collaborative-filtering/assets/55864839/db49e7aa-a172-46bd-819a-9ecb7bab60cc)

### **Spark RDD - Item-based Collaborative Filtering.ipynb**

The **Spark RDD - Item-based Collaborative Filtering.ipynb** is recommended to run on the Jupyter Notebook powered by an **[Amazon EMR cluster](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html)**. This recommendation is due to the potential limitation of local machine resources, which may not have sufficient computing power to efficiently process a large movie dataset with 1 million rows using PySpark.

Running the item-based collaborative filtering algorithm on a local machine for such a large dataset might result in extended processing times and potential memory issues. On the other hand, leveraging an Amazon EMR cluster allows you to distribute the computation across multiple nodes, significantly speeding up the processing and handling the data-intensive task more efficiently.

To run the **Spark RDD - Item-based Collaborative Filtering.ipynb** on an Amazon EMR cluster, follow these steps:

1. Set up an Amazon EMR cluster with the necessary configuration and number of nodes to handle the workload efficiently.
2. Upload the notebook to the cluster's master node.
3. Access the Jupyter Notebook running on the cluster's master node via your web browser.
4. Execute the notebook on the cluster, benefiting from the distributed computing capabilities.

Alternatively, you can convert the notebook into a Python script and run it from the master node of the EMR cluster (i.e. Master public DNS) using the following command:

```
spark-submit --executor-memory 1g [Spark-driver-script-name].py
```

(Note: Since the default executor memory might not be sufficient for processing one million movie ratings, it is essential to specify the executor memory to be 1GB.)

### **Spark UI**

After executing the Spark driver script, you can access the Spark UI via `localhost:4040`, which can be helpful for debugging purposes.

<br>

## **Contribution**

Contributions to this project are welcome! If you have any ideas for improvements or bug fixes, please feel free to open an issue or submit a pull request.

<br>

## **License**

This project is licensed under the Apache License 2.0 and the MIT License - see the LICENSE file for details.

<br>

## **Acknowledgement**

I extend my gratitude to [PySpark](https://spark.apache.org/docs/latest/api/python/#:~:text=PySpark%20is%20the%20Python%20API,for%20interactively%20analyzing%20your%20data.) and [Apache Spark](https://spark.apache.org/) communities for their valuable contributions to the data processing and analysis ecosystem.
