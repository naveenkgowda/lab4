# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 100

#Path to the dataset, modify this
data_path = "../data/vehicle{}.csv"

#Path to your certificates, modify this
#certificate_formatter = "./certificates/device_{}/device_{}.certificate.pem"
#key_formatter = "./certificates/device_{}/device_{}.private.pem"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a1pmo6ektwsh2y-ats.iot.us-east-1.amazonaws.com", 8883)
        self.client.configureCredentials("root-ca-cert.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO3: fill in the function to show your received message
        print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, Payload="payload"):
        #TODO4: fill in this function for your publish
        if Payload.__contains__("veh0"):
            self.client.subscribeAsync("vehicle0/data/maxco2", 0, ackCallback=self.customSubackCallback)
            self.client.publishAsync("vehicle0/data", Payload, 0, ackCallback=self.customPubackCallback)
        elif Payload.__contains__("veh1"):
            self.client.subscribeAsync("vehicle1/data/maxco2", 0, ackCallback=self.customSubackCallback)
            self.client.publishAsync("vehicle1/data", Payload, 0, ackCallback=self.customPubackCallback)
        elif Payload.__contains__("veh2"):
            self.client.subscribeAsync("vehicle2/data/maxco2", 0, ackCallback=self.customSubackCallback)
            self.client.publishAsync("vehicle2/data", Payload, 0, ackCallback=self.customPubackCallback)
        elif Payload.__contains__("veh3"):
            self.client.subscribeAsync("vehicle3/data/maxco2", 0, ackCallback=self.customSubackCallback)
            self.client.publishAsync("vehicle3/data", Payload, 0, ackCallback=self.customPubackCallback)
        elif Payload.__contains__("veh4"):
            self.client.subscribeAsync("vehicle4/data/maxco2", 0, ackCallback=self.customSubackCallback)
            self.client.publishAsync("vehicle4/data", Payload, 0, ackCallback=self.customPubackCallback)
        

print("Loading vehicle data...")

#load Vehicle_0 data
data_vehicle0 = pd.read_csv("../data/vehicle0.csv")

#load Vehicle_1 data
data_vehicle1 = pd.read_csv("../data/vehicle1.csv")

#load Vehicle_2 data
data_vehicle2 = pd.read_csv("../data/vehicle2.csv")

#load Vehicle_3 data
data_vehicle3 = pd.read_csv("../data/vehicle3.csv")

#load Vehicle_4 data
data_vehicle4 = pd.read_csv("../data/vehicle4.csv")


print("Initializing MQTTClients...")
#Initialize vehicle0
clients = []
client = MQTTClient("vehicle_0", "ec6b1380cb.cert.pem","ec6b1380cb.private.key")
client.client.connect()
clients.append(client)

#Initialize vehicle1
client = MQTTClient("vehicle1", "93d9f2e9cd.cert.pem","93d9f2e9cd.private.key")
client.client.connect()
clients.append(client)

#Initialize vehicle2
client = MQTTClient("vehicle2", "63b8d7683f.cert.pem","63b8d7683f.private.key")
client.client.connect()
clients.append(client)

#Initialize vehicle3
client = MQTTClient("vehicle3", "72a27be84b.cert.pem","72a27be84b.private.key")
client.client.connect()
clients.append(client)

#Initialize vehicle4
client = MQTTClient("vehicle4", "77eea04304.cert.pem","77eea04304.private.key")
client.client.connect()
clients.append(client)

index =0 

while True:
    print("send now?")
    x = input()
    if x == "s":
        for i,c in enumerate(clients):
            if c.device_id == "vehicle_0":
                payload = data_vehicle0.iloc[index:index+1, :].to_json(orient="records")
                c.publish(payload)
            elif c.device_id == "vehicle1":
                payload = data_vehicle1.iloc[index:index+1, :].to_json(orient="records")
                c.publish(payload)
            elif c.device_id == "vehicle2":
                payload = data_vehicle2.iloc[index:index+1, :].to_json(orient="records")
                c.publish(payload)
            elif c.device_id == "vehicle3":
                payload = data_vehicle3.iloc[index:index+1, :].to_json(orient="records")
                c.publish(payload)
            elif c.device_id == "vehicle4":
                payload = data_vehicle4.iloc[index:index+1, :].to_json(orient="records")
                c.publish(payload)
            else:
                print("Device ID not found")
            # increment the index
            

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    index = index+1
    time.sleep(3)
    





