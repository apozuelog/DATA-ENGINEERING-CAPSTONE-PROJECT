<img src="img/logo.PNG" alt="Logo"/>  

# A Data Engineer project.  

**What is Ford GoBike?**
Ford GoBike is the Bay Area's bike share system. Bay Area Bike Share was introduced in 2013 as a pilot program for the region, with 700 bikes and 70 stations across San Francisco and San Jose. Once expansion is complete, Ford GoBike will grow to 7,000 bikes across San Francisco, the East Bay and San Jose.  

<img src="img/mapa.PNG" alt="Maph" style="width: 400px;"/>

Ford GoBike, like other bike share systems, consists of a fleet of specially designed, sturdy and durable bikes that are locked into a network of docking stations throughout the city. The bikes can be unlocked from one station and returned to any other station in the system, making them ideal for one-way trips. People use bike share to commute to work or school, run errands, get to appointments or social engagements and more. It's a fun, convenient and affordable way to get around.

![maph](img/bici.jpg)

The bikes are available for use 24 hours/day, 7 days/week, 365 days/year and riders have access to all bikes in the network when they become a member or purchase a pass.


### Scope the Project and Gather Data  

The objective of optimizing the number of bicycles in each station and studying the behavior of the GoBike user.
After the exploratory analysis of the data, I have chosen to create a DataWarehouse in Redshift for storage and exploitation of the data using BI tools such as Quicksight or Tableau. The ETL will be developed with Airflow to automate the upload through task.
In principle, a large number of users is not expected for the exploitation of the data.

Steps:
1. Raw storage of files (in our case CSV) in AWS S3.
2. Exploratory analysis with PySpark
3. Clustering and output of data in Parquet format
4. Definition of the data model
5. Create dimension tables and fact tables in Redshift
6. Development in Airflow of the necessary operators to load data in our Redshift model  

<img src="img/aws_achitecture.PNG" alt="GoBike architecture"/>

#### 1. Raw storage of files in AWS S3
En nuestro S3 tenemos una serie de csv con la información que generan las bicicletas compartidas GoBike.  
Total size: 558.4 MB in 16 objects.

#### 2. Exploratory analysis with PySpark
Mediante el Jupyter Noetbook 
He desarrollado **bikes_trips.py** 
Ejecutando este archivo, creamos una **SparkSession** para realizar el análisis exploratorio para decantarnos por el modelo de datos apropiado y el proceso ETL. 

la arquitectura de ca y posteriormente guardar en archivo parquet (no se ha implementado porque los tiempos de carga son muy altos).  
Creamos dataset con los registros para cada tabla.

This first dataset has more than 2.7 million records in 16 columns and includes records from 2018-01-01 to 2019-09-30.
Este es el schema:   

**STAGING**  
![schema](img/staging_schema.PNG)  

**DIM TABLES**  
![schema](img/time_table_schema.PNG)  
![schema](img/station_table_schema.PNG)  
![schema](img/user_table_schema.PNG)  
![schema](img/bikes_table_schema.PNG)  

**FACT TABLE**  
![schema](img/bike_trips_table_schema.PNG)

### Step 3: Define the Data Model
El modelo es una bbdd en estrella con 4 tablas de dimensiones, una de hechos y staging
**aquí imagen en estrella**

### Step 4: Run ETL to Model the Data  

* **StageToRedshiftOperator**
* **LoadDimensionOperator**
* **LoadFactOperator**
* **DataQualityOperator**  

![schema](img/graph_execution.PNG)

### Step 5: Complete Project Write Up  
**Tiempos de carga**  
Aquí Texto  
![schema](img/gant_execution.PNG)  

**Todos los task OK!!**  
Aquí Texto  
![schema](img/tree_view.PNG)  

### EXTRA BONUS  
**Quicksight dashboard**  
![schema](img/quicksight.PNG) 
