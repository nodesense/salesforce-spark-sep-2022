# SPARK

```
wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz

tar xf spark-3.1.3-bin-hadoop3.2.tgz

sudo mv spark-3.1.3-bin-hadoop3.2 /opt

sudo chmod 777 /opt/spark-3.1.3-bin-hadoop3.2
```


# KAFKA
```
wget http://packages.confluent.io/archive/5.5/confluent-5.5.5-2.12.tar.gz

tar xf confluent-5.5.5-2.12.tar.gz

sudo mv confluent-5.5.5 /opt

sudo chmod 777 /opt/confluent-5.5.5

```

# For BASH Shell

```
touch  ~/.bashrc

echo "export SPARK_HOME=/opt/spark-3.1.3-bin-hadoop3.2" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >>  ~/.bashrc


echo "export KAFKA_HOME=/opt/confluent-5.5.5" >> ~/.bashrc
echo "export PATH=\$PATH:\$KAFKA_HOME/bin" >>  ~/.bashrc

```


# For ZSH Shell

```
touch  ~/.zshrc

echo "export SPARK_HOME=/opt/spark-3.1.3-bin-hadoop3.2" >> ~/.zshrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >>  ~/.zshrc


echo "export KAFKA_HOME=/opt/confluent-5.5.5" >> ~/.zshrc
echo "export PATH=\$PATH:\$KAFKA_HOME/bin" >>  ~/.zshrc

```

# GET STARTED 


# V.Env do this for every terminal

```
source ~/.bashrc

python3 -m venv  spark-dev

cd spark-dev

source ./bin/activate

```

```
pip install findspark
pip install jupyterlab

```

open new terminal to start pyspark

```
source ~/.bashrc

pyspark 

```

place below code to test..

```
sc.parallelize ( range (1, 10) ).collect()
```

# to start kafka

open new terminal

```
source ~/.bashrc
zookeeper-server-start /opt/confluent-5.5.5/etc/kafka/zookeeper.properties

```

open new terminal 

```
source ~/.bashrc
kafka-server-start /opt/confluent-5.5.5/etc/kafka/server.properties
```

# To start jupyter lab

open new terminal

```
source ~/.bashrc

cd spark-dev

source ./bin/activate

jupyter lab
```

it should open a brower, if not, check jupyter console output, like http://localhost:8888?token=blahblah... copy and open in the broweser

Click new python3 icon 

paste the code from below url and run it

https://github.com/nodesense/cts-bigdata-apr-2022/blob/main/pyspark-notebooks/S001-HelloWorld.ipynb
