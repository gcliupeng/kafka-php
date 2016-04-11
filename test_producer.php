<?php
require_once('/root/kafka-php/autoload.php');
$kafka_config = array( 
	  'KF_ZK_HOSTS' => '10.10.10.117:2181/kafka/GSTest01',
      'KF_BROKER_HOSTS' => '10.10.10.117:9093,10.10.10.117:9092,10.10.10.111:9092,10.10.10.113:9092',
      'KF_ZK_TIMEOUT' => 1000,
      'KF_SEND_TIMEOUT' => 100
      );
$zk_hosts = $kafka_config['KF_ZK_HOSTS'];
$broker_hosts = $kafka_config['KF_BROKER_HOSTS'];
$zk_timeout = $kafka_config['KF_ZK_TIMEOUT'];
$send_timeout = $kafka_config['KF_SEND_TIMEOUT'];
$produce = Kafka\Produce::getInstance($zk_hosts, $zk_timeout, $broker_hosts);
//$consumer = Kafka\Consumer::getInstance($this->_zk_hosts, $this->_zk_timeout);        
//$re=$produce->getClient()->getTopicDetail("liupeng2",true);
$produce->setTimeOut($send_timeout);
$produce->setMessages("liupeng2", 3, array("hello"));
$produce->setRequireAck(1);
$re=$produce->send(1);
var_dump($re);
?>
