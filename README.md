# SPARK

```
wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz

tar xf spark-3.1.3-bin-hadoop3.2.tgz

sudo mv spark-3.1.3-bin-hadoop3.2 /opt

sudo chmod 777 /opt/spark-3.1.3-bin-hadoop3.2
```


# KAFKA
 
wget http://packages.confluent.io/archive/5.5/confluent-5.5.5-2.12.tar.gz
tar xf confluent-5.5.5-2.12.tar.gz

sudo mv confluent-5.5.5 /opt

sudo chmod 777 /opt/confluent-5.5.5


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
