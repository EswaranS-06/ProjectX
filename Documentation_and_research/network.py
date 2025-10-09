from scapy.all import *
import random
import time
from prettytable import PrettyTable
import tensorflow as tf
import numpy as np
from collections import namedtuple
import struct
import os

# Custom GTP Header class
class GTPHeader(Packet):
    name = "GTP Header"
    fields_desc = [
        BitField("version", 1, 3),
        BitField("PT", 1, 1),
        BitField("reserved", 0, 1),
        BitField("E", 0, 1),
        BitField("S", 0, 1),
        BitField("PN", 0, 1),
        ByteField("message_type", 255),
        ShortField("length", None),
        IntField("teid", 0),
    ]

    def post_build(self, p, pay):
        if self.length is None:
            l = len(pay)
            # length field counts the length of the payload, excluding the GTP header (8 bytes)
            # It is stored in bytes 2 and 3 (index 2 and 3 of p)
            p = p[:2] + struct.pack("!H", l) + p[4:]
        return p + pay


# Function to generate a packet based on the traffic type
def generate_packet(src_ip, dst_ip, src_port, dst_port, traffic_type, payload_message):
    # Generate packet payload based on the provided message
    payload = payload_message.encode('utf-8')

    # Generate random MAC addresses for 802.11 packet
    src_mac = ":".join([f"{random.randint(0, 255):02x}" for _ in range(6)])
    dst_mac = ":".join([f"{random.randint(0, 255):02x}" for _ in range(6)])

    # Create the packet based on traffic type
    if traffic_type == "802.11":
        packet = RadioTap() / Dot11(type=2, subtype=0, addr1=dst_mac, addr2=src_mac, addr3=dst_mac) / LLC() / SNAP() / IP(src=src_ip, dst=dst_ip) / TCP(sport=src_port, dport=dst_port) / payload
    elif traffic_type in ["3G", "4G", "5G NR"]:
        packet = IP(src=src_ip, dst=dst_ip) / UDP(sport=src_port, dport=2152) / GTPHeader(teid=random.randint(0, 4294967295)) / IP(src=src_ip, dst=dst_ip) / UDP(sport=src_port, dport=dst_port) / payload
    else:
        raise ValueError("Invalid traffic type")

    return packet


# Function to simulate traffic through proxy and reverse proxy
def simulate_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, interval, proxy_ip, proxy_port, reverse_proxy_ip, reverse_proxy_port, payload_messages):
    sent_packets = []
    received_packets = []

    # Check for root/admin privileges to safely send packets
    can_send = os.geteuid() == 0 if hasattr(os, 'geteuid') else True  # Windows does not have geteuid

    # Generate and send packets through proxy and reverse proxy for each traffic type
    for traffic_type in ["802.11", "3G", "4G", "5G NR"]:
        for i in range(num_packets):
            # Get the payload message for the current packet
            payload_message = payload_messages[i % len(payload_messages)]

            # Send packet through proxy
            proxy_packet = generate_packet(src_ip, proxy_ip, src_port, proxy_port, traffic_type, payload_message)
            if can_send:
                send(proxy_packet, verbose=False)
            sent_packets.append((proxy_packet, traffic_type))

            # Send packet through reverse proxy
            reverse_proxy_packet = generate_packet(proxy_ip, reverse_proxy_ip, proxy_port, reverse_proxy_port, traffic_type, payload_message)
            if can_send:
                send(reverse_proxy_packet, verbose=False)
            sent_packets.append((reverse_proxy_packet, traffic_type))

            # Send packet from reverse proxy to destination
            dst_packet = generate_packet(reverse_proxy_ip, dst_ip, reverse_proxy_port, dst_port, traffic_type, payload_message)
            if can_send:
                send(dst_packet, verbose=False)
            sent_packets.append((dst_packet, traffic_type))

            # Simulate receiving packets
            received_packet = generate_packet(dst_ip, reverse_proxy_ip, dst_port, reverse_proxy_port, traffic_type, payload_message)
            received_packets.append((received_packet, traffic_type))
            received_packet = generate_packet(reverse_proxy_ip, proxy_ip, reverse_proxy_port, proxy_port, traffic_type, payload_message)
            received_packets.append((received_packet, traffic_type))
            received_packet = generate_packet(proxy_ip, src_ip, proxy_port, src_port, traffic_type, payload_message)
            received_packets.append((received_packet, traffic_type))

            time.sleep(interval)

    return sent_packets, received_packets


# Function to print the simulation results in a table format
def print_results(sent_packets, received_packets, src_ip, dst_ip, proxy_ip, reverse_proxy_ip):
    table = PrettyTable()
    table.field_names = ["Type", "Traffic Type", "Source IP", "Destination IP", "Source Port", "Destination Port", "Proxy IP", "Reverse Proxy IP"]

    for packet, traffic_type in sent_packets:
        if traffic_type == "802.11":
            table.add_row(["Sent", traffic_type, packet[IP].src, packet[IP].dst, packet[TCP].sport, packet[TCP].dport, proxy_ip, reverse_proxy_ip])
        else:
            table.add_row(["Sent", traffic_type, packet[IP].src, packet[IP].dst, packet[UDP].sport, packet[UDP].dport, proxy_ip, reverse_proxy_ip])

    for packet, traffic_type in received_packets:
        if traffic_type == "802.11":
            table.add_row(["Received", traffic_type, packet[IP].src, packet[IP].dst, packet[TCP].sport, packet[TCP].dport, proxy_ip, reverse_proxy_ip])
        else:
            table.add_row(["Received", traffic_type, packet[IP].src, packet[IP].dst, packet[UDP].sport, packet[UDP].dport, proxy_ip, reverse_proxy_ip])

    print(table)
    print(f"Total Packets Sent: {len(sent_packets)}")
    print(f"Total Packets Received: {len(received_packets)}")


# Function to generate 802.11 traffic
def generate_802_11_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages):
    packets = []
    for i in range(num_packets):
        payload_message = payload_messages[i % len(payload_messages)]
        packet = generate_packet(src_ip, dst_ip, src_port, dst_port, "802.11", payload_message)
        packets.append(packet)
    return packets


# Function to generate 3G traffic
def generate_3g_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages):
    packets = []
    for i in range(num_packets):
        payload_message = payload_messages[i % len(payload_messages)]
        packet = generate_packet(src_ip, dst_ip, src_port, dst_port, "3G", payload_message)
        packets.append(packet)
    return packets


# Function to generate 4G traffic
def generate_4g_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages):
    packets = []
    for i in range(num_packets):
        payload_message = payload_messages[i % len(payload_messages)]
        packet = generate_packet(src_ip, dst_ip, src_port, dst_port, "4G", payload_message)
        packets.append(packet)
    return packets


# Function to generate 5G NR traffic
def generate_5g_nr_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages):
    packets = []
    for i in range(num_packets):
        payload_message = payload_messages[i % len(payload_messages)]
        packet = generate_packet(src_ip, dst_ip, src_port, dst_port, "5G NR", payload_message)
        packets.append(packet)
    return packets


# Function to print a sample of the generated traffic
def print_traffic_sample(traffic_type, packets):
    print(f"\n{traffic_type} Traffic Sample:")
    for packet in packets[:3]:
        packet.show()


# Function to convert packets to features and labels for TensorFlow dataset
def packet_to_features(packet):
    features = {
        "src_ip": tf.train.Feature(int64_list=tf.train.Int64List(value=list(map(int, packet[IP].src.split("."))))),
        "dst_ip": tf.train.Feature(int64_list=tf.train.Int64List(value=list(map(int, packet[IP].dst.split("."))))),
        "src_port": tf.train.Feature(int64_list=tf.train.Int64List(value=[packet[TCP].sport if TCP in packet else packet[UDP].sport])),
        "dst_port": tf.train.Feature(int64_list=tf.train.Int64List(value=[packet[TCP].dport if TCP in packet else packet[UDP].dport])),
        "payload_length": tf.train.Feature(int64_list=tf.train.Int64List(value=[len(packet[Raw].load) if Raw in packet else 0]))
    }
    label = tf.train.Feature(int64_list=tf.train.Int64List(value=[0]))  # Placeholder label
    return tf.train.Example(features=tf.train.Features(feature=features)), label.int64_list.value[0]


# Function to create TensorFlow dataset from packet data
def create_tensorflow_dataset(packets):
    serialized_examples = []
    labels_list = []
    for packet in packets:
        example, label = packet_to_features(packet)
        serialized_examples.append(example.SerializeToString())
        labels_list.append(label)

    dataset = tf.data.Dataset.from_tensor_slices((serialized_examples, labels_list))

    feature_description = {
        "src_ip": tf.io.FixedLenFeature([4], tf.int64),
        "dst_ip": tf.io.FixedLenFeature([4], tf.int64),
        "src_port": tf.io.FixedLenFeature([], tf.int64),
        "dst_port": tf.io.FixedLenFeature([], tf.int64),
        "payload_length": tf.io.FixedLenFeature([], tf.int64)
    }

    def _parse_function(example_proto, label):
        features = tf.io.parse_single_example(example_proto, feature_description)
        return features, label

    dataset = dataset.map(_parse_function)

    return dataset


# Function to create data loader from TensorFlow dataset
def create_data_loader(dataset, batch_size):
    data_loader = dataset.batch(batch_size)
    return data_loader


# Main function
def main():
    # Configuration
    src_ip = "10.0.0.1"
    dst_ip = "192.168.0.1"
    src_port = 1234
    dst_port = 80
    num_packets = 5  # Number of packets to generate for each traffic type
    interval = 1  # Interval between packets in seconds
    proxy_ip = "172.16.0.1"
    proxy_port = 8080
    reverse_proxy_ip = "172.16.0.2"
    reverse_proxy_port = 8081

    # Payload messages representing a mobile user browsing an e-commerce site
    payload_messages = [
        "GET /products HTTP/1.1\r\nHost: www.example.com\r\n\r\n",
        "GET /products/1 HTTP/1.1\r\nHost: www.example.com\r\n\r\n",
        "GET /cart HTTP/1.1\r\nHost: www.example.com\r\n\r\n",
        "POST /cart/add HTTP/1.1\r\nHost: www.example.com\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 15\r\n\r\nproduct_id=1&quantity=1",
        "GET /checkout HTTP/1.1\r\nHost: www.example.com\r\n\r\n"
    ]

    # Generate synthetic traffic for each protocol
    wifi_packets = generate_802_11_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages)
    gsm_packets = generate_3g_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages)
    lte_packets = generate_4g_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages)
    nr_packets = generate_5g_nr_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, payload_messages)

    # Print a portion of each synthetic traffic
    print_traffic_sample("802.11", wifi_packets)
    print_traffic_sample("3G GSM", gsm_packets)
    print_traffic_sample("4G LTE", lte_packets)
    print_traffic_sample("5G NR", nr_packets)

    # Start traffic simulation
    print("Starting traffic simulation...")
    sent_packets, received_packets = simulate_traffic(src_ip, dst_ip, src_port, dst_port, num_packets, interval, proxy_ip, proxy_port, reverse_proxy_ip, reverse_proxy_port, payload_messages)
    print("Traffic simulation completed.")

    # Print results
    print_results(sent_packets, received_packets, src_ip, dst_ip, proxy_ip, reverse_proxy_ip)

    # Create TensorFlow datasets for each traffic type
    wifi_dataset = create_tensorflow_dataset(wifi_packets)
    gsm_dataset = create_tensorflow_dataset(gsm_packets)
    lte_dataset = create_tensorflow_dataset(lte_packets)
    nr_dataset = create_tensorflow_dataset(nr_packets)

    # Create data loaders for each traffic type
    batch_size = 32
    wifi_data_loader = create_data_loader(wifi_dataset, batch_size)
    gsm_data_loader = create_data_loader(gsm_dataset, batch_size)
    lte_data_loader = create_data_loader(lte_dataset, batch_size)
    nr_data_loader = create_data_loader(nr_dataset, batch_size)

    # Safely compute and print dataset sizes by iterating rather than len()
    print("wifi dataset size:", sum(1 for _ in wifi_dataset))
    print("gsm dataset size:", sum(1 for _ in gsm_dataset))
    print("lte dataset size:", sum(1 for _ in lte_dataset))
    print("nr dataset size:", sum(1 for _ in nr_dataset))


if __name__ == '__main__':
    main()
