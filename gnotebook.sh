export PYSPARK_DRIVER_PYTHON=ipython 
export PYSPARK_DRIVER_PYTHON_OPTS="notebook" 
pyspark --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11
