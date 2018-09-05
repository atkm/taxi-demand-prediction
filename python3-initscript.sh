set -e
set -u

echo -n "Installing python3 .. "
apt-get update
apt-get -y install python3-pip
pip3 install numpy
echo "Done."

# configure spark to use python3.6
# ref: https://stackoverflow.com/questions/45843960/how-to-run-python3-on-googles-dataproc-pyspark?rq=1
echo "export PYSPARK_PYTHON=python3" | tee -a  /etc/profile.d/spark_config.sh  /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "export PYTHONHASHSEED=0" | tee -a /etc/profile.d/spark_config.sh /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf

echo -n "Setting up the prediction project .. "
git clone https://akumano@bitbucket.org/akumano/taxi-demand-prediction.git
# Spark jobs should get data from gs://.
#gsutil cp gs://nyc-taxi-8472/yellow_tripdata_2014-{01..12}_tiny.csv ./data/
#gsutil cp gs://nyc-taxi-8472/lga_2014-{01..12}.csv ./data/
echo "Done."
