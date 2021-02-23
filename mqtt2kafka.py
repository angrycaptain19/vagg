
from kafka import KafkaProducer
import paho.mqtt.client as mqtt
import json
#kafka -> mqtt -> spark
mqtt_topic = 'test1/test'

kafka_topic = 'test1'

def sendmessage2kafka(messages):
    print('send message to kafka', messages)
    producer.send(kafka_topic, messages)



def mqtt2kafka():
    print('test1 successful')
    client = mqtt.Client('mqtt2kafka')
    on_connect = lambda client, userdata, flags, rc: client.subscribe(mqtt_topic)
    client.on_connect

    on_message = lambda client, userdata, message: sendmessagetokafka(message.payload.decode())
    client.on_message

    on_disconnect = lambda client, userdata, flags, rc: print('disconnected', client, userdata, flags, rc)
    client.on_disconnect

    client.connect('localhost', 1883, 10)
    client.loop_forever()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version = (0,10, 1), value_serializer = lambda x: dumps(x).encode('utf-8'))
    mqtt2kafka()
