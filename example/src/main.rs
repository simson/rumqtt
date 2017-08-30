extern crate rumqtt;

///
///  This is a simple example which demonstrates using the rumqtt client lib
///  both within a thread with a mpsc queue, and also using the message
///  callback directly. 
/// 
///  Note that the input loop is also the inbound message reader loop
///  for Client1's messages. This means that it won't actually check
///  messages from Client1 until input has been received. 
///  Enter a blank line to flush the queue. 
///  
///  In this example, if you want a message to be printed by by a 
///  particular client, prefix the message with the client name.  
///  If you want to to be printed by both, prefix it with all/

use std::io::{self, BufRead};
use std::sync::Arc;
use std::sync::mpsc::{ sync_channel, TryRecvError };
use std::thread;
use std::string::String;
use rumqtt::{QoS, Message, MqttClient, MqttCallback, MqttOptions};

type Topic = String;
type Msg = String;

fn main() {

    let (inmsg_tx, inmsg_rx) = sync_channel::<(Topic, Msg)>(512);
    let (outmsg_tx, outmsg_rx) = sync_channel::<(Topic, Msg)>(512);

    // client number 1
    thread::spawn(move|| {
        let opts = MqttOptions::new()
            .set_broker("localhost:1883")
            .set_reconnect(10);

        let msg_cb = move |msg : Message| {
            let topic : String = msg.topic.into();
            let message = String::from_utf8(msg.payload.to_vec()).unwrap();
            inmsg_tx.send((topic, message)).unwrap();
        };

        let cbs = MqttCallback {
            on_message: Some(Arc::new(Box::new(msg_cb))),
            on_publish: None,
        };

        let mut client = MqttClient::start(opts, Some(cbs)).unwrap();
        client.subscribe(vec![("all/#", QoS::Level1), ("client1/#", QoS::Level1)]).unwrap();

        loop {
            match outmsg_rx.recv() {
                Ok((topic, msg)) => {
                    if topic == "quit" {
                        return;
                    }
                    client.publish(topic.as_str(), QoS::Level1, msg.into_bytes()).unwrap();
                },
                Err(_) => { // queue is closed. Time to go. 
                    return;
                }
            }
        }
    });
       
    // client number 2
    let opts = MqttOptions::new()
        .set_broker("localhost:1883")
        .set_reconnect(10);

    let msg_cb = move |msg : Message| {
        let topic : String = msg.topic.into();
        let message = String::from_utf8(msg.payload.to_vec()).unwrap();
        println!("Client 2 : Received message '{}' on topic '{}'", message, topic);
    };

    let cbs = MqttCallback {
        on_message: Some(Arc::new(Box::new(msg_cb))),
        on_publish: None,
    };

    let mut client2 = MqttClient::start(opts, Some(cbs)).unwrap();
    client2.subscribe(vec![("all/#", QoS::Level1), ("client2/#", QoS::Level1)]).unwrap();
   
    println!("Please type a message in the form of [topic/path] [my message]");
    println!("Topic can contain / but no spaces.  Message can contain spaces.");
    println!("Client1 is subscribed to all/# and client1/#");
    println!("Client2 is subscribed to all/# and client2/#");
    println!("Start a line with . to quit");

    let stdin = io::stdin();
    let mut iterator = stdin.lock().lines();
    loop {
        match inmsg_rx.try_recv() {
            Ok((topic, msg)) => {
                println!("Client 1 : Received message '{}' on topic '{}'", msg, topic);
            },
            Err(TryRecvError::Disconnected) => {
                println!("Queue is closed. Time to go");
                break;
            },
            _ => { 
                // only other option is Empty, so carry on.
            }
        }

        let mut buffer = iterator.next().unwrap().unwrap();

        if buffer.len() == 0 {
            continue;
        }

        if buffer.starts_with(".") {
            outmsg_tx.send(("quit".into(), String::new())).unwrap();
            println!("Thanks for playing!");
            return;
        } else {
            if let Some(idx) = buffer.find(" ") {
                let msg  = buffer.split_off(idx);
                println!("Sending msg to client1 thread");
                outmsg_tx.send((buffer, msg)).unwrap();
            } else {
                println!("Invalid input. Format should be:  [topic] [message]");
            }
        }
    }
}
