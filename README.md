# DATA ENGINEERING CAPSTONE PROJECT 

![logo](/img/logo.png)  

**What is Ford GoBike?**
Ford GoBike is the Bay Area's bike share system. Bay Area Bike Share was introduced in 2013 as a pilot program for the region, with 700 bikes and 70 stations across San Francisco and San Jose. Once expansion is complete, Ford GoBike will grow to 7,000 bikes across San Francisco, the East Bay and San Jose.  

![maph](img/mapa.PNG)

Ford GoBike, like other bike share systems, consists of a fleet of specially designed, sturdy and durable bikes that are locked into a network of docking stations throughout the city. The bikes can be unlocked from one station and returned to any other station in the system, making them ideal for one-way trips. People use bike share to commute to work or school, run errands, get to appointments or social engagements and more. It's a fun, convenient and affordable way to get around.

![maph](img/bici.jpg)

The bikes are available for use 24 hours/day, 7 days/week, 365 days/year and riders have access to all bikes in the network when they become a member or purchase a pass.


#### Step 1: Scope the Project and Gather Data  

El scoop de este proyecto es realizar un proceso de carga de datos en bruto en nuestro Data Lake en AWS S3, análisis y limpieza de datos, diseño de BBDD en AWS Redshift y creación del proceso ETL con AirFlow para guardar los registros en las distintas tablas.

##### AWS S3
En nuestro S3 tenemos una serie de csv con la información que generan las bicicletas compartidas GoBike.  
Total size: 558.4 MB in 16 objects.

#### Step 2: Explore and Assess the Data
**bikes_trips.py**  
Ejecutando este archivo, creamos una **SparkSession** para cargar, analizar, limpiar y postariormente guardar en archivo parquet (no se ha implementado porque los tiempos de carga son muy altos).  
Creamos dataset con los registros para cada tabla.

This first dataset has more than 2.7 million records in 16 columns and includes records from 2018-01-01 to 2019-09-30.
Este es el schema:  
`# Number of records and schema
print(df.count())
df.printSchema()`


#### Step 3: Define the Data Model

#### Step 4: Run ETL to Model the Data

#### Step 5: Complete Project Write Up
