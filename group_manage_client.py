import socket
import os
import multiprocessing as mp
import time
import json

print(os.getpid())

#g = raw_input("Enter your name : ") 
#print(g)
g = "a"

def node_status_check(node_id):
    port = (10000) + node_id
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try: 
        client.connect(('0.0.0.0', port))
    except socket.error, e:
        print("Socket error: ", e)
        node_status[node_id-1] = 0
        node_age[node_id-1] = 0
        return
    status_check_message = '{"activity":"node_status_check","checked_node_id":'+str(node_id)+'}'
    client.send(status_check_message)
    from_server = client.recv(4096)
    rcvd_mssg = json.loads(from_server)
    node_status[node_id-1] = 1
    node_age[node_id-1] = rcvd_mssg["age"]
    client.close()
    print('My port is :', port, 'the age of the pinged node is: ', node_age[node_id-1], ' seconds')
    #print(node_age)

def group_update(node_id, elected_leader_id):
    print("Group update initiated...................................")
    port = (10000) + node_id
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try: 
        client.connect(('0.0.0.0', port))
    except socket.error, e:
        print("Socket error: ", e)
        node_status[node_id-1] = 0
        node_age[node_id-1] = 0
        return
    status_mssg = ' '.join([str(elem) for elem in node_status])
    #print(status_mssg)
    send_update_message = '{"activity":"group_update","leader_node_id":'+str(elected_leader_id)+',"node_status":"'+status_mssg+'"}'
    client.send(send_update_message)
    from_server = client.recv(4096)
    print(from_server)
    #node_status[node_id-1] = True
    #node_age[node_id-1] = rcvd_mssg["age"]
    client.close()
    #print('My port is :', port, 'I got the message: ', node_age[node_id-1])

def write_request(elected_leader_id, value_to_be_written):
    print("Write request initiated...................................")
    port = (10000) + elected_leader_id
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try: 
        client.connect(('0.0.0.0', port))
    except socket.error, e:
        print("Socket error: ", e)
        return
    send_write_message = '{"activity":"write_request","value_to_be_written":"'+ value_to_be_written+'"}'
    client.send(send_write_message)
    from_server = client.recv(4096)
    rcvd_mssg = from_server
    print('Received write message udpate statue from Leader node: ', rcvd_mssg)
    #node_status[node_id-1] = True
    #node_age[node_id-1] = rcvd_mssg["age"]
    client.close()
    #print('My port is :', port, 'I got the message: ', node_age[node_id-1])

def read_request(elected_leader_id):
    print("Read request initiated...................................")
    port = (10000) + elected_leader_id
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try: 
        client.connect(('0.0.0.0', port))
    except socket.error, e:
        print("Socket error: ", e)
        return
    send_write_message = '{"activity":"read_request"}'
    client.send(send_write_message)
    from_server = client.recv(4096)
    rcvd_mssg = from_server
    print('Read result: ', rcvd_mssg)
    #node_status[node_id-1] = True
    #node_age[node_id-1] = rcvd_mssg["age"]
    client.close()
    #print('My port is :', port, 'I got the message: ', node_age[node_id-1])

    

manager = mp.Manager()
node_age = manager.list([0]*3)
node_status = manager.list([0]*3)
#node_age = [0,0,0]
status_check_procs = []
group_update_procs = []
nodes = [1,2,3]

for i in nodes:
    #print i
    p = mp.Process(target=node_status_check, args=(i,))
    status_check_procs.append(p)
    p.start()
    p.join()
    #time.sleep(2)

time.sleep(2)
#leader election
print(node_status)
print(node_age)
leader_node_id = node_age.index(max(node_age))+1
print('The leader elected is Node ', leader_node_id)

for j in nodes:
    #print i
    #if node_status[j]==1:
    p = mp.Process(target=group_update, args=(j,leader_node_id))
    group_update_procs.append(p)
    p.start()
    p.join()

time.sleep(5)
write_request(leader_node_id, g)

time.sleep(5)
read_request(leader_node_id)


    #    pass
# while (as all the 3 processes finish)
    #update the group management status and send the message to leader
        # leader broadcasts to all if it's write and returns value if read 