# Poke Price Tracker Project 
### **A Fully Orchestradted ETL Pipeline Project**
![alt text](https://static.wixstatic.com/media/cc40a2_917feedd900e41c2a98e9aee302ac588~mv2.jpg/v1/fill/w_439,h_440,al_c,q_80,usm_0.66_1.00_0.01,enc_auto/_5d016c03-e3f2-46cd-89b7-6191fe2f769d_ed.jpg)

If you want to **know more** about the **project** and its **goals**, **read** this **[article](https://jeanguinvarch.wixsite.com/jean-guinvarch-s-por/s-projects-side-by-side)** I have written ! 
https://jeanguinvarch.wixsite.com/jean-guinvarch-s-por/s-projects-side-by-side

### My Socials:
* [linkedin](https://www.linkedin.com/in/jean-guinvarch-80b35620a/)
* [medium](https://medium.com/@jean_gvh)
* [soundcloud](https://soundcloud.com/diligencexs)
* [email](jean.guinvarch@gmail.com)

# Purposes
**Primary objectives** of this project was to systematically **collect eBay auction sales data**,

specifically focusing on tracking the evolving prices of Pokémon cards(over a period of one year or more).![alt text](https://static.wixstatic.com/media/cc40a2_c46b064810824bd2be4270d8eedfc772~mv2.png/v1/fill/w_439,h_246,al_c,q_85,usm_0.66_1.00_0.01,enc_auto/cc40a2_c46b064810824bd2be4270d8eedfc772~mv2.png)

# The Data Stack 
In order to build a completly autonomous data pipeline, a coherent data stack is necessary to tackle this problem.
### Here is how it looks

![alt text](https://static.wixstatic.com/media/cc40a2_b41c2fbc532043b1a3a12ae8b1a6c85e~mv2.png/v1/fill/w_840,h_346,al_c,lg_1,q_85,enc_auto/cc40a2_b41c2fbc532043b1a3a12ae8b1a6c85e~mv2.png)

# How it works ? 

### Data Collection

* **Daily data** is **collected** using **web scraping** with the **Beautiful Soup** (BS4) library in Python.

### Staging

* **Raw collected data** is **converted** into a **CSV file** and **stored in a staging area**, specifically a **Google Cloud Platform (GCP) bucket**.

###  Data Transformation & Cleaning

* **Staging data** is retrieved for further **transformation** & **cleaning**.

###  Export to Cloud SQL

* **Processed data** is **exported** to another **GCP bucket** in CSV format, prepared for **transfer to a Cloud SQL database**.

###  Database Transfer

* The **data** is **split into various dataframes** using **Pandas**, following the **database schema**.

* **Exported dataframes** are **transferred** to the **Cloud SQL database**.

### Visualization

* The **data** is **visualized** on a **dashboard** using **Power BI**, with **automatic refresh functionality**.

###  Orchestration

* All these **steps** are **exectuting** in the **cloud** thanks to **Apache Airflow** ( Composer on GCP)​

# Thank you for reading !


