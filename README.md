# Massive_Data_Analysis_Projects (Currently Re-Editing)
Projects with practical application in the course of *Massive Data Analysis (CS573200)* at National Tsing Hua University.

The projects are implemented in Java code with Hadoop Map-Reduce structure, running on Hortonworks Sandbox, and the actual implementation includes **PageRank**, **Locality-Sensitivity Hashing**, **KMeans**, and **Frequent Itemsets**.

## Prerequisite
* [Hortonworks Sandbox for Hadoop](https://www.cloudera.com/downloads/hortonworks-sandbox.html)
* [Virtual Box](https://www.virtualbox.org/)

## Project Introduction
### PageRank
A brief introduction on [PageRank](https://www.youtube.com/watch?v=TSGQ4F1E6H8).
![PageRank formula](https://github.com/YungChengHsu/Massive_Data_Analysis_Projects/blob/main/PageRank/Project_description.png)

* The input data set can be downloaded [here](https://snap.stanford.edu/data/p2p-Gnutella04.html)
* There are 10876 nodes in the network graph
* The project lists out the top 10 node with the highest page rank as the image shows:
  ![result]()
  
 ### Locality-Sensitivity Hashing (LSH)
 This project implements the application of **Finding Similar Items**, and the a brief introduction on the process (3 parts) is as follows:
 1. [Part 1](https://www.youtube.com/watch?v=c6xK9WgRFhI)
 2. [Part 2](https://www.youtube.com/watch?v=96WOGPUgMfw)
 3. [Part 3](https://www.youtube.com/watch?v=_1D35bN95Go)
 
 ![project intro](https://github.com/YungChengHsu/Massive_Data_Analysis_Projects/blob/main/Locality-Sensitive_Hashing/Project_description.png)
 
 There are 3 main parts of the project:
 1. **Shingling**
    1. Read in the articles and create shingles of 3 words (3-shingles).   For example, there will be 4 3-shingles from the sentence "I commit java code on github", and they are "I commit java", "commit java code", "java code on", "code on github".
 2. **Min-hashing**
 3. **LSH**
 
 ### KMeans
 
 ### Frequent Itemsets
