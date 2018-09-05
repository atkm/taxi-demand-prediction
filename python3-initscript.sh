set -e
set -u

echo -n "Installing python3.6 .. "
# install python3.6 and virtualenv from testing repo
echo 'deb http://ftp.de.debian.org/debian testing main' | sudo tee --append /etc/apt/sources.list
echo 'APT::Default-Release "stable";' | sudo tee -a /etc/apt/apt.conf.d/00local
apt-get update
apt-get -t testing -y install python3.6 python3-venv libc6-dev git # update libc6-dev to avoid breaking dependencies

# venv unnecessary? -> nice because it comes with pip of the right version.
python3.6 -m venv venv
source ./venv/bin/activate
pip install numpy
echo "Done."

# configure spark to use python3.6
# ref: https://stackoverflow.com/questions/45843960/how-to-run-python3-on-googles-dataproc-pyspark?rq=1
echo "export PYSPARK_PYTHON=python3.6" | tee -a  /etc/profile.d/spark_config.sh  /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "export PYTHONHASHSEED=0" | tee -a /etc/profile.d/spark_config.sh /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf

echo -n "Setting up the prediction project .. "
git clone https://akumano@bitbucket.org/akumano/taxi-demand-prediction.git
cd taxi-demand-prediction
gsutil cp gs://nyc-taxi-8472/yellow_tripdata_2014-{01..12}_tiny.csv ./data/
gsutil cp gs://nyc-taxi-8472/lga_2014-{01..12}.csv ./data/
echo "Done."
