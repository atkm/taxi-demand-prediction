# install python3.6 and virtualenv from testing repo
echo 'deb http://ftp.de.debian.org/debian testing main' | sudo tee --append /etc/apt/sources.list
echo 'APT::Default-Release "stable";' | sudo tee -a /etc/apt/apt.conf.d/00local
apt-get update
apt-get -t testing -y install python3.6 python3-venv libc6-dev git # update libc6-dev to avoid breaking dependencies

python3.6 -m venv venv
source ./venv/bin/activate

echo "export PYSPARK_PYTHON=python3.6" | tee -a  /etc/profile.d/spark_config.sh  /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "export PYTHONHASHSEED=0" | tee -a /etc/profile.d/spark_config.sh /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf
