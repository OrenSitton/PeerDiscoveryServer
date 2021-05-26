"""
Author: Oren Sitton
File: Peer Discovery Server.py
Python Version: 3.8
Description: Server used by nodes to discover other nodes in the network (peer discovery)
"""
import logging
import queue
import socket
import sys

import select


def initialize_server(ip, port):
    """
    initializes server socket object to address,
    non-blocking and to accept new connections
    :param ip: ipv4 address
    :type ip: str
    :param port: tcp port
    :type port: int
    :return: initialized server socket
    :rtype: socket.socket
    """

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setblocking(False)
    server.bind((ip, port))
    server.listen(5)

    logging.info("Server initiated at address ({}, {})"
                 .format(server.getsockname()[0], server.getsockname()[1]))

    return server


def hexify(number, length):
    """
    calculates hexadecimal value of the number, with prefix zeroes to match length
    :param number: number to calculate hex value for, in base 10
    :type number: int
    :param length: requested length of hexadecimal value
    :type length: int
    :return: hexadecimal value of the number, with prefix zeroes
    :rtype: str
    :raise Exception: ValueError (message size is larger than length)
    """
    if not isinstance(number, int):
        raise TypeError("Transaction.hexify(number, length): expected number to be of type int")
    if not isinstance(length, int):
        raise TypeError("Transaction.hexify(number, length): expected length to be of type int")
    if number < 0:
        raise ValueError("Transaction.hexify(number, length): expected non-negative value for number, received {} "
                         "instead".format(number))
    if length < 0:
        raise ValueError("Transaction.hexify(number, length): expected non-negative value for length, received {} "
                         "instead".format(length))

    hex_base = hex(number)[2:]

    if len(hex_base) <= length:
        hex_base = (length - len(hex_base)) * "0" + hex_base
        return hex_base
    else:
        raise ValueError("Transaction.hexify(number, length): message size is larger than length")


def handle_message(request, ip_addresses, destination):
    """
    handles incoming messages from clients. Analyzes client request and creates appropriate response.
    :param request: message received from client
    :type request: str
    :param ip_addresses: current stored addresses of nodes in the network
    :type ip_addresses: list
    :param destination: address of the client who sent the request
    :type destination: str
    :return: reply to send to client
    :rtype: str
    """
    ip_addresses = ip_addresses.copy()
    request_type = request[:1]

    if request_type == "a":
        if destination in ip_addresses and ip_addresses[len(ip_addresses) - 1] != destination:
            ip_addresses[ip_addresses.index(destination)] = ip_addresses[len(ip_addresses) - 1]
            # replace requester's address with a different address

        peer_count = 0
        reply = ""

        for x in ip_addresses[:-1]:
            if x != "":
                peer_count += 1
                if x == "localhost":
                    reply += "{}{}{}{}".format(hexify(127, 2), hexify(0, 2), hexify(0, 2), hexify(1, 2))
                else:
                    x = x.split(".")
                    reply += "{}{}{}{}".format(
                        hexify(int(x[0]), 2), hexify(int(x[1]), 2), hexify(int(x[2]), 2), hexify(int(x[3]), 2))

        reply = "b{}{}".format(hexify(peer_count, 2), reply)

    else:
        reply = "f{}".format("unrecognized request".encode("utf-8").hex())

    length = hexify(len(reply), 5)

    reply = "{}{}".format(length, reply)

    return reply


def main():
    if len(sys.argv) != 3:
        raise RuntimeError("Peer Discovery Server requires exactly 2 arguments")

    ip = "0.0.0.0"
    port = int(sys.argv[1])

    # maximum amount of addresses server will send nodes
    addresses_amount = 3

    # list to store node addresses
    ip_addresses = []

    # amount of ip addresses currently stored
    count = int(sys.argv[2])
    for x in range(addresses_amount + 1):
        ip_addresses.append("")

    server_socket = initialize_server(ip, port)

    # lists of sockets to listen to
    inputs = [server_socket]
    outputs = []

    # lists of messages to send to clients
    message_queues = {}

    while inputs:
        readable, writable, exceptional = select.select(inputs, outputs, inputs)

        for sock in readable:
            if sock is server_socket:
                connection, client_address = server_socket.accept()
                connection.setblocking(False)
                inputs.append(connection)

                logging.info("Connected to client at ({}, {})"
                             .format(client_address[0], client_address[1]))

                if client_address[0] not in ip_addresses:
                    ip_addresses[count] = client_address[0]
                    count += 1
                    count %= addresses_amount + 1

            else:
                try:
                    data = sock.recv(5).decode()
                    if data:
                        logging.info("Received message from client at ({}, {})"
                                     .format(sock.getpeername()[0], sock.getpeername()[1]))
                        data = sock.recv(int(data, 16)).decode()
                        if sock in message_queues:
                            message_queues[sock].put(data)
                        else:
                            message_queues[sock] = queue.Queue()
                            message_queues[sock].put(data)

                        if sock not in outputs:
                            outputs.append(sock)

                    else:
                        logging.info("Disconnected from client at ({}, {})"
                                     .format(sock.getpeername()[0], sock.getpeername()[1]))

                        if sock in outputs:
                            outputs.remove(sock)
                        inputs.remove(sock)
                        sock.close()
                        if sock in message_queues:
                            del message_queues[sock]

                except ConnectionResetError:
                    logging.info("An existing connection was forcibly closed by the client: ({}, {})"
                                 .format(sock.getpeername()[0], sock.getpeername()[1]))

                    if sock in outputs:
                        outputs.remove(sock)
                    inputs.remove(sock)
                    sock.close()
                    if sock in message_queues:
                        del message_queues[sock]

        for sock in writable:
            if not message_queues[sock].empty():
                next_msg = message_queues[sock].get()
                reply = handle_message(next_msg, ip_addresses, sock.getpeername()[0])
                sock.send(reply.encode())
                logging.info("Sent message to client at ({}, {})"
                             .format(sock.getpeername()[0], sock.getpeername()[1]))

                if message_queues[sock].empty():
                    del message_queues[sock]
                    outputs.remove(sock)

        for sock in exceptional:
            inputs.remove(sock)
            if sock in outputs:
                outputs.remove(sock)
            sock.close()
            del message_queues[sock]
            logging.info("Disconnected from client at ({}, {})"
                         .format(sock.getpeername()[0], sock.getpeername()[1]))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(message)s")
    main()
