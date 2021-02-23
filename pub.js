
// MQTT publisher
var mqtt = require('mqtt')
var client = mqtt.connect('mqtt://localhost:1883')
var topic = 'test1/test'
var message = 'Hello World!'

client.on('connect', ()=>{
    console.log("connected");
    client.publish('presence', 'message');
    setInterval(()=>{
        client.publish(topic, message)
        console.log('Message sent!', message)
    }, 1000)
})

