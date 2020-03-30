import socket
import multiprocessing as mp
import time
import json

node_id = 1
stored_string = ""
nodes = [1,2,3]

manager = mp.Manager()
node_wise_write_update = manager.list([0]*3)

node_status = [0,0,0]
leader_node_id = 0

start = time.time()

def convert(string): 
    global node_status
    node_status = list(string.split(" "))
    for i in range(0, len(node_status)):
        node_status[i] = int(node_status[i])
    #print(node_status)
    #return li

def connect_with_followers(leader_id, node_id):
    port = ((leader_id+10)*1000) + node_id
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Leader connected with socket: ', port)
    try:
        client.connect(('0.0.0.0', port))
    except socket.error, e:
        print("Socket error: ", e)
        node_wise_write_update[node_id-1]=0
        return
    message_to_followers = stored_string
    client.send(message_to_followers)
    from_server = client.recv(4096)
    client.close()
    print(from_server)
    #if int(from_server)==1:
    if (from_server=="1"):
        node_wise_write_update[node_id-1]=1


def initiate_multicast():
    # pop up multiple processes
    print('Multicast initiated...................................')
    global nodes
    global node_status
    global leader_node_id
    multi_cast_procs = []
    for i in range(0,len(nodes)):
        #print(' For i: ', i)
        # connect to node only if it's active and the node is not the leader itself
        print(' For i: ', i, ' Node status: ', node_status[i], ' node number: ', nodes[i])
        if (node_status[i]==1 and nodes[i]!=leader_node_id):
            p = mp.Process(target=connect_with_followers, args=(leader_node_id,nodes[i],))
            multi_cast_procs.append(p)
            p.start()
            p.join()
        #time.sleep(2)
    time.sleep(2)
    # check if all active nodes were updated with the latest write
    write_transaction_fail_count = 0
    for j in range(0,len(nodes)):
        print(' For j: ', j, ' Node status: ', node_status[j], ' node_wise_write_update: ', node_wise_write_update[j])
        if write_transaction_fail_count>0:
            return 0 # indicating failure
        if j == (leader_node_id-1):
            continue # skip for the leader itself
        if (node_status[j]==1 and node_wise_write_update[j]==0):
            write_transaction_fail_count = write_transaction_fail_count + 1
    return 1
            


def server_connect(node_id): 
    # printing process id
    global node_status
    global leader_node_id
    global stored_string
    temp_variable = False
    port = (10000) + node_id
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.bind(('0.0.0.0', port))
    print('Listening at: ', port)
    serv.listen(5)
    while True:
        temp_variable = False
        conn, addr = serv.accept()
        from_client = ''
        while True:
            data = conn.recv(4096)
            if not data: break
            from_client += data
            print(from_client)
            rcvd_mssg = json.loads(from_client)
            if rcvd_mssg["activity"]=="node_status_check":
                print('Node status check received.....................................')
                age = time.time() - start
                response_message = '{"age":' + str(round(age,2)) + '}'
                conn.send(response_message)
            if rcvd_mssg["activity"]=="group_update":
                print('Group update received...........................................')
                #if rcvd_mssg["leader_node_id"]==node_id:
                response_message = 'Group update received by Node ' + str(node_id)
                conn.send(response_message)
                # update local copy of leader_node_id
                leader_node_id = int(rcvd_mssg["leader_node_id"])
                # update local copy of node_status
                convert(rcvd_mssg["node_status"])
                print('updated local copy of node_status: ', node_status)
                if rcvd_mssg["leader_node_id"]!=node_id:
                    # close connection as the next following function call exits the function
                    temp_variable = True
                    #conn.shutdown()
                    #conn.close()
                    #print('Connection closed at listeners end (listener to client)....................................')
                    # act as follower (listen to leader) (single process)
                    #listen_to_leader(leader_node_id, node_id)
                    #print("ignore me")
            if rcvd_mssg["activity"]=="write_request":
                print('Write request received...........................................')
                # only possible when this node is leader
                # act as leader and send to all nodes (client function). Need to run multiple processes
                result = initiate_multicast()
                print("Write status update from all followers: ", result)
                # update the local copy of the leader only if it's working for all
                stored_string = stored_string + rcvd_mssg["value_to_be_written"]
                response_message = str(result)
                conn.send(response_message)
            if rcvd_mssg["activity"]=="read_request":
                # only possible when the node is the leader
                response_message = stored_string
                conn.send(response_message)
            #message = "I am node " + str(node_id) + 'alive since ' + str(age)
            #if activity = write and leader = this node, then update array and run leader broadcast function
            #if activity = write and leader = others, then run server function to listen to broadcasts and confirm back if updated array
            
        conn.close()
        print('Connection closed at listeners end (listener to client)....................................')
        if temp_variable==True:
            print('temp variable is true.....')
            #listen_to_leader(leader_node_id, node_id)
        #print('sending from: ', port)

def listen_to_leader(leader_id, node_id):
    # printing process id
    global stored_string
    print("Acting as a follower to Node: ", leader_id, "..............................")
    port = ((leader_id+10)*1000) + node_id
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.bind(('0.0.0.0', port))
    print('Listening to the leader at port : ', port)
    serv.listen(5)
    while True:
        conn, addr = serv.accept()
        from_leader = ''
        while True:
            data = conn.recv(4096)
            if not data: break
            from_leader += data
            print('Received message from Node ', leader_id, ' : ', from_leader)
            stored_string = stored_string + from_leader
            # just update local copy of the array (as a follower)
            print('Local copy updated to ', stored_string)
            #message = '{"age":' + str(round(age,2)) + '}'
            #message = "received update at node: " + str(node_id)
            message="1"
            conn.send(message)
        conn.close()
        print('Connection closed at listeners end (listener to leader)....................................')
        #print('sending from: ', port)
#def leader_broadcast():

#p1 = mp.Process(target=server_connect, args=(node_id,))
#p1.start()

server_connect(node_id)



