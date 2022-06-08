#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ThreadPool.h"
#include "my_common.h"

#define SERV_PORT 8411
#define BACKLOG 32
#define MAX_BUF_SIZE 131072

// Use secondary poll if define it
// #define USE_SECONDARY_POOL

// Print log
bool my_debug = false;
bool my_info = false;
bool my_error = true;
#define LOG(on, Format...) \
  if (on) {                \
    printf(Format);        \
  }

using namespace std;

struct client_resources {
  rdma_cm_id *id = nullptr;
  ibv_pd *pd = nullptr;
  ibv_comp_channel *comp_channel = nullptr;
  ibv_cq *cq = nullptr;
  ibv_qp_init_attr qp_init_attr{};
  ibv_qp *qp = nullptr;
};

struct memory_resources {
  ibv_mr *client_metadata_mr = nullptr, *server_buffer_mr = nullptr;
  rdma_buffer_attr client_metadata_attr{};
  ibv_recv_wr client_recv_wr, *bad_client_recv_wr = nullptr;
  ibv_send_wr server_send_wr, *bad_server_send_wr = nullptr;
  ibv_sge client_recv_sge{}, server_send_sge{};
};

static void print_array_data(void * buffer) {
  unsigned long result_length = *(unsigned long *)buffer;
  char *result_data = (char *)buffer + sizeof(unsigned long);
  result_data[result_length] = '\0';
  printf("\nBuffer length:%lu, data:%s\n", result_length, result_data);
}

/* When we call this function cm_client_id must be set to a valid identifier.
 * This is where, we prepare client connection before we accept it. This
 * mainly involve pre-posting a receive buffer to receive client side
 * RDMA credentials
 */
static bool setup_client_resources(client_resources &client_rc) {
  /* We have a valid connection identifier, lets start to allocate
   * resources. We need:
   * 1. Protection Domains (PD)
   * 2. Memory Buffers
   * 3. Completion Queues (CQ)
   * 4. Queue Pair (QP)
   * Protection Domain (PD) is similar to a "process abstraction"
   * in the operating system. All resources are tied to a particular PD.
   * And accessing recourses across PD will result in a protection fault.
   */
  client_rc.pd = ibv_alloc_pd(client_rc.id->verbs 
			/* verbs defines a verb's provider, 
			 * i.e an RDMA device where the incoming 
			 * client connection came */);
  if (!client_rc.pd) {
    LOG(my_error, "Failed to allocate a protection domain!\n");
    return false;
  }
  printf("A new protection domain is allocated at %p \n", client_rc.pd);
  /* Now we need a completion channel, were the I/O completion
   * notifications are sent. Remember, this is different from connection
   * management (CM) event notifications.
   * A completion channel is also tied to an RDMA device, hence we will
   * use cm_client_id->verbs.
   */
  client_rc.comp_channel = ibv_create_comp_channel(client_rc.id->verbs);
  if (!client_rc.comp_channel) {
    LOG(my_error, "Failed to create an I/O completion event channel!\n");
    return false;
  }
  printf("An I/O completion event channel is created at %p \n",
        client_rc.comp_channel);
  /* Now we create a completion queue (CQ) where actual I/O
   * completion metadata is placed. The metadata is packed into a structure
   * called struct ibv_wc (wc = work completion). ibv_wc has detailed
   * information about the work completion. An I/O request in RDMA world
   * is called "work" ;)
   */
  client_rc.cq = ibv_create_cq(
      client_rc.id->verbs /* which device*/, CQ_CAPACITY /* maximum capacity*/,
      NULL /* user context, not used here */,
      client_rc.comp_channel /* which IO completion channel */,
      0 /* signaling vector, not used here*/);
  if (!client_rc.cq) {
    LOG(my_error, "Failed to create a completion queue (cq)!\n");
    return false;
  }
  printf("Completion queue (CQ) is created at %p with %d elements \n",
        client_rc.cq, client_rc.cq->cqe);
  /* Ask for the event for all activities in the completion queue*/
  if (ibv_req_notify_cq(client_rc.cq /* on which CQ */,
                        0 /* 0 = all event type, no filter*/)) {
    LOG(my_error, "Failed to request notifications on CQ!\n");
    return false;
  }
  /* Now the last step, set up the queue pair (send, recv) queues and their
   * capacity. The capacity here is define statically but this can be probed
   * from the device. We just use a small number as defined in rdma_common.h */
  bzero(&client_rc.qp_init_attr, sizeof(client_rc.qp_init_attr));
  client_rc.qp_init_attr.cap.max_recv_sge =
      MAX_SGE; /* Maximum SGE per receive posting */
  client_rc.qp_init_attr.cap.max_recv_wr =
      MAX_WR; /* Maximum receive posting capacity */
  client_rc.qp_init_attr.cap.max_send_sge =
      MAX_SGE; /* Maximum SGE per send posting */
  client_rc.qp_init_attr.cap.max_send_wr =
      MAX_WR; /* Maximum send posting capacity */
  client_rc.qp_init_attr.qp_type =
      IBV_QPT_RC; /* QP type, RC = Reliable connection */
  /* We use same completion queue, but one can use different queues */
  client_rc.qp_init_attr.recv_cq =
      client_rc
          .cq; /* Where should I notify for receive completion operations */
  client_rc.qp_init_attr.send_cq =
      client_rc.cq; /* Where should I notify for send completion operations */
  /*Lets create a QP */
  if (rdma_create_qp(client_rc.id /* which connection id */,
                     client_rc.pd /* which protection domain*/,
                     &client_rc.qp_init_attr /* Initial attributes */)) {
    LOG(my_error, "Failed to create QP!\n");
    return false;
  }
  /* Save the reference for handy typing but is not required */
  client_rc.qp = client_rc.id->qp;
  printf("Client QP created at %p\n", client_rc.qp);
  return true;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static bool accept_client_connection(rdma_event_channel *cm_event_channel,
                                     const client_resources &client_rc,
                                     memory_resources &mem_rc) {
  struct rdma_conn_param conn_param;
  struct rdma_cm_event *cm_event = NULL;
  struct sockaddr_in remote_sockaddr;
  if (!client_rc.id || !client_rc.qp) {
    LOG(my_error, "Client resources are not properly setup\n");
    return false;
  }
  /* we prepare the receive buffer in which we will receive the client
   * metadata*/
  mem_rc.client_metadata_mr = rdma_buffer_register(
      client_rc.pd /* which protection domain */,
      &mem_rc.client_metadata_attr /* what memory */,
      sizeof(mem_rc.client_metadata_attr) /* what length */,
      (IBV_ACCESS_LOCAL_WRITE) /* access permissions */);
  if (!mem_rc.client_metadata_mr) {
    LOG(my_error, "Failed to register client attr buffer\n");
    // we assume ENOMEM
    return false;
  }
  /* We pre-post this receive buffer on the QP. SGE credentials is where we
   * receive the metadata from the client */
  mem_rc.client_recv_sge.addr =
      (uint64_t)mem_rc.client_metadata_mr->addr;  // same as &client_buffer_attr
  mem_rc.client_recv_sge.length = mem_rc.client_metadata_mr->length;
  mem_rc.client_recv_sge.lkey = mem_rc.client_metadata_mr->lkey;
  printf("Regist client_recv_sge addr=%p, length=%u\n",
        mem_rc.client_recv_sge.addr, mem_rc.client_recv_sge.length);
  /* Now we link this SGE to the work request (WR) */
  bzero(&mem_rc.client_recv_wr, sizeof(mem_rc.client_recv_wr));
  mem_rc.client_recv_wr.sg_list = &mem_rc.client_recv_sge;
  mem_rc.client_recv_wr.num_sge = 1;  // only one SGE
  if (ibv_post_recv(client_rc.qp /* which QP */,
                    &mem_rc.client_recv_wr /* receive work request*/,
                    &mem_rc.bad_client_recv_wr /* error WRs */)) {
    LOG(my_error, "Failed to pre-post the receive buffer\n");
    return false;
  }
  printf("Receive buffer pre-posting is successful \n");
  /* Now we accept the connection. Recall we have not accepted the connection
   * yet because we have to do lots of resource pre-allocation */
  memset(&conn_param, 0, sizeof(conn_param));
  /* this tell how many outstanding requests can we handle */
  conn_param.initiator_depth =
      CONN_DEPTH; /* For this exercise, we put a small number here */
  /* This tell how many outstanding requests we expect other side to handle */
  conn_param.responder_resources =
      CONN_DEPTH; /* For this exercise, we put a small number */
  if (rdma_accept(client_rc.id, &conn_param)) {
    LOG(my_error, "Failed to accept the connection\n");
    return false;
  }
  /* We expect an RDMA_CM_EVNET_ESTABLISHED to indicate that the RDMA
   * connection has been established and everything is fine on both, server
   * as well as the client sides.
   */
  printf("Going to wait for : RDMA_CM_EVENT_ESTABLISHED event \n");
  if (process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ESTABLISHED,
                            &cm_event)) {
    LOG(my_error, "Failed to get the cm event\n");
    return false;
  }
  /* We acknowledge the event */
  if (rdma_ack_cm_event(cm_event)) {
    LOG(my_error, "Failed to acknowledge the cm event\n");
    return false;
  }
  /* Just FYI: How to extract connection information */
  memcpy(&remote_sockaddr /* where to save */,
         rdma_get_peer_addr(client_rc.id) /* gives you remote sockaddr */,
         sizeof(struct sockaddr_in) /* max size */);
  printf("A new connection is accepted from %s \n",
         inet_ntoa(remote_sockaddr.sin_addr));
  return true;
}

/* Wait until we receive client buffer metadata*/
static bool wait_receive_client_metadata(client_resources &client_rc,
                                         memory_resources &mem_rc) {
  struct ibv_wc wc;
  /* Now, we first wait for the client to start the communication by
   * sending the server its metadata info. The server does not use it
   * in our example. We will receive a work completion notification for
   * our pre-posted receive request.
   */
  if (process_work_completion_events(client_rc.comp_channel, &wc, 1) != 1) {
    LOG(my_error, "Failed to receive from client!\n");
    return false;
  }
  /* if all good, then we should have client's buffer information, lets see */
  printf("Client side buffer information is received...\n");
  show_rdma_buffer_attr(&mem_rc.client_metadata_attr);
  printf("The client buffer's size : %u bytes \n",
         mem_rc.client_metadata_attr.length);
  return true;
}

/* Regist server buffer as 'dst' of RDMA_READ and 'src' of RDMA_WRITE */
static bool regist_server_buffer(void *buf, client_resources &client_rc,
                                 memory_resources &mem_rc) {
  /* We need to setup requested memory buffer. This is where the client will
   * do RDMA READs and WRITEs. */
  mem_rc.server_buffer_mr = rdma_buffer_register(
      client_rc.pd /* which protection domain */, buf /* what memory*/,
      mem_rc.client_metadata_attr.length /* what size to allocate */,
      (enum ibv_access_flags)(
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
          IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
  if (!mem_rc.server_buffer_mr) {
    LOG(my_error, "Server failed to create a buffer!\n");
    /* we assume that it is due to out of memory error */
    return false;
  }
  return true;
}

/* RDMA_READ: Receive serialized CompactionParam to server buffer */
static bool receive_compaction_param(client_resources &client_rc,
                                     memory_resources &mem_rc) {
  struct ibv_wc wc;
  /* Now we prepare a READ */
  mem_rc.server_send_sge.addr = (uint64_t)mem_rc.server_buffer_mr->addr;
  mem_rc.server_send_sge.length = (uint32_t)mem_rc.server_buffer_mr->length;
  mem_rc.server_send_sge.lkey = mem_rc.server_buffer_mr->lkey;
  /* now we link to the send work request */
  bzero(&mem_rc.server_send_wr, sizeof(mem_rc.server_send_wr));
  mem_rc.server_send_wr.sg_list = &mem_rc.server_send_sge;
  mem_rc.server_send_wr.num_sge = 1;
  mem_rc.server_send_wr.opcode = IBV_WR_RDMA_READ;
  mem_rc.server_send_wr.send_flags = IBV_SEND_SIGNALED;
  /* we have to tell client side info for RDMA */
  mem_rc.server_send_wr.wr.rdma.rkey =
      mem_rc.client_metadata_attr.stag.remote_stag;
  mem_rc.server_send_wr.wr.rdma.remote_addr =
      mem_rc.client_metadata_attr.address;
  /* Now we post it */
  if (ibv_post_send(client_rc.qp, &mem_rc.server_send_wr,
                    &mem_rc.bad_server_send_wr)) {
    LOG(my_error, "Failed to read client dst buffer from the master\n");
    return false;
  }
  /* at this point we are expecting 1 work completion for the write */
  if (process_work_completion_events(client_rc.comp_channel, &wc, 1) != 1) {
    LOG(my_error,
        "We failed to get 1 work completions (RDMA_READ from client)\n");
    return false;
  }
  printf("receive_compaction_param RDMA_READ from client success.\n");
  return true;
}

/* RDMA_WRITE: Receive serialized CompactionParam to server buffer */
static bool send_compaction_result(client_resources &client_rc,
                                   memory_resources &mem_rc) {
  struct ibv_wc wc;
  /* Now we prepare a WRITE */
  mem_rc.server_send_sge.addr = (uint64_t)mem_rc.server_buffer_mr->addr;
  mem_rc.server_send_sge.length = (uint32_t)mem_rc.server_buffer_mr->length;
  mem_rc.server_send_sge.lkey = mem_rc.server_buffer_mr->lkey;
  /* now we link to the send work request */
  bzero(&mem_rc.server_send_wr, sizeof(mem_rc.server_send_wr));
  mem_rc.server_send_wr.sg_list = &mem_rc.server_send_sge;
  mem_rc.server_send_wr.num_sge = 1;
  mem_rc.server_send_wr.opcode = IBV_WR_RDMA_WRITE;
  mem_rc.server_send_wr.send_flags = IBV_SEND_SIGNALED;
  /* we have to tell client side info for RDMA */
  mem_rc.server_send_wr.wr.rdma.rkey =
      mem_rc.client_metadata_attr.stag.remote_stag;
  mem_rc.server_send_wr.wr.rdma.remote_addr =
      mem_rc.client_metadata_attr.address;
  /* Now we post it */
  if (ibv_post_send(client_rc.qp, &mem_rc.server_send_wr,
                    &mem_rc.bad_server_send_wr)) {
    LOG(my_error, "Failed to read client dst buffer from the master\n");
    return false;
  }
  /* at this point we are expecting 1 work completion for the write */
  if (process_work_completion_events(client_rc.comp_channel, &wc, 1) != 1) {
    LOG(my_error,
        "We failed to get 1 work completions (RDMA_WRITE to client)\n");
    return false;
  }
  printf("send_compaction_result RDMA_WRITE to client success.\n");
  return true;
}

// /* This function sends server side buffer metadata to the connected client */
// static bool send_server_metadata_to_client(const client_resources &client_rc,
//                                            memory_resources &mem_rc) {
//   struct ibv_wc wc;
//   /* This buffer is used to transmit information about the above
//    * buffer to the client. So this contains the metadata about the server
//    * buffer. Hence this is called metadata buffer. Since this is already
//    * on allocated, we just register it.
//    * We need to prepare a send I/O operation that will tell the
//    * client the address of the server buffer.
//    */
//   mem_rc.server_metadata_attr.address =
//   (uint64_t)mem_rc.server_buffer_mr->addr; mem_rc.server_metadata_attr.length
//   =
//       (uint32_t)mem_rc.server_buffer_mr->length;
//   mem_rc.server_metadata_attr.stag.local_stag =
//       (uint32_t)mem_rc.server_buffer_mr->lkey;
//   mem_rc.server_metadata_mr = rdma_buffer_register(
//       client_rc.pd /* which protection domain*/,
//       &mem_rc.server_metadata_attr /* which memory to register */,
//       sizeof(mem_rc.server_metadata_attr) /* what is the size of memory */,
//       IBV_ACCESS_LOCAL_WRITE /* what access permission */);
//   if (!mem_rc.server_metadata_mr) {
//     LOG(my_error, "Server failed to create to hold server metadata!\n");
//     /* we assume that it is due to out of memory error */
//     return false;
//   }
//   /* We need to transmit this buffer. So we create a send request.
//    * A send request consists of multiple SGE elements. In our case, we only
//    * have one
//    */
//   mem_rc.server_send_sge.addr = (uint64_t)&mem_rc.server_metadata_attr;
//   mem_rc.server_send_sge.length = sizeof(mem_rc.server_metadata_attr);
//   mem_rc.server_send_sge.lkey = mem_rc.server_metadata_mr->lkey;
//   /* now we link this sge to the send request */
//   bzero(&mem_rc.server_send_wr, sizeof(mem_rc.server_send_wr));
//   mem_rc.server_send_wr.sg_list = &mem_rc.server_send_sge;
//   mem_rc.server_send_wr.num_sge = 1;  // only 1 SGE element in the array
//   mem_rc.server_send_wr.opcode = IBV_WR_SEND;  // This is a send request
//   mem_rc.server_send_wr.send_flags =
//       IBV_SEND_SIGNALED;  // We want to get notification
//   /* This is a fast data path operation. Posting an I/O request */
//   if (ibv_post_send(
//       client_rc.qp /* which QP */,
//       &mem_rc.server_send_wr /* Send request that we prepared before */,
//       &mem_rc.bad_server_send_wr /* In case of error, this will contain
//       failed requests */)) {
//     LOG(my_error, "Posting of server metdata failed!\n");
//     return false;
//   }
//   /* We check for completion notification */
//   if (process_work_completion_events(client_rc.comp_channel, &wc, 1) != 1) {
//     LOG(my_error, "Failed to send server metadata!\n");
//     return false;
//   }
//   printf("Local buffer metadata has been sent to the client \n");
//   return true;
// }

/* This is server side logic. Server passively waits for the client to call
 * rdma_disconnect() and then it will clean up its resources */
static bool disconnect_and_cleanup(rdma_event_channel *cm_event_channel,
                                   client_resources &client_rc,
                                   memory_resources &mem_rc) {
  struct rdma_cm_event *cm_event = NULL;
  /* Active DISCONNECT from server side */
  if (rdma_disconnect(client_rc.id)) {
    LOG(my_error, "Failed to disconnect\n");
  }
  /* Now we wait for the client to send us disconnect event */
  printf("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
  if (process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_DISCONNECTED,
                            &cm_event)) {
    LOG(my_error, "Failed to get disconnect event\n");
  }
  /* We acknowledge the event */
  if (rdma_ack_cm_event(cm_event)) {
    LOG(my_error, "Failed to acknowledge the cm event\n");
  }
  printf("A disconnect event is received from the client...\n");
  /* We free all the resources */
  /* Destroy QP */
  rdma_destroy_qp(client_rc.id);
  /* Destroy client cm id */
  if (rdma_destroy_id(client_rc.id)) {
    LOG(my_error, "Failed to destroy client id cleanly\n");
    // we continue anyways;
  }
  /* Destroy CQ */
  if (ibv_destroy_cq(client_rc.cq)) {
    LOG(my_error, "Failed to destroy completion queue cleanly\n");
    // we continue anyways;
  }
  /* Destroy completion channel */
  if (ibv_destroy_comp_channel(client_rc.comp_channel)) {
    LOG(my_error, "Failed to destroy completion channel cleanly\n");
    // we continue anyways;
  }
  /* Destroy memory buffers */
  rdma_buffer_deregister(mem_rc.client_metadata_mr);
  rdma_buffer_deregister(mem_rc.server_buffer_mr);
  /* Destroy protection domain */
  if (ibv_dealloc_pd(client_rc.pd)) {
    LOG(my_error, "Failed to destroy client protection domain cleanly\n");
    // we continue anyways;
  }
  return true;
}

// Parse PluggableCompactionParam From Proto
static void ParseCompactionParamFromProto(string &param,
                                          const string &proto_param) {
  param = proto_param + "_Parsed_Param";
}

// Serialize PluggableCompactionResult To Proto
static void SerializeCompactionResultToProto(string &proto_result,
                                             const string &result) {
  proto_result = result + "_Serialized_Result";
}

// Get Secondary DBPath
static string GetSecondaryDBPath(const string &primary_db_name, const int tid) {
  return primary_db_name + "_secondary_" + to_string(tid);
}

class RemoteCompactionServer {
 public:
  explicit RemoteCompactionServer(int thread_poll_size, int port)
      : port_(port), pool_(thread_poll_size) {
    bzero(&my_addr, sizeof(my_addr));  // set first 8 bytes to zero
    my_addr.sin_family = AF_INET;      // AF_INET address
    my_addr.sin_port = htons(port);    // setup port(host -> networks)
    my_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // ipv4 address

#ifdef USE_SECONDARY_POOL
    for (int t = 0; t < thread_poll_size; t++) {
      secondary_dbs_.emplace_back(std::unordered_map<string, DB *>());
    }
#endif
  }

  ~RemoteCompactionServer() = default;

  // Bind and Listen
  void SetupServer() {
    try {
      /*  Open a channel used to report asynchronous communication event */
      if (!(cm_event_channel_ = rdma_create_event_channel())) {
        throw "rdma_create_event_channel error!";
      }
      /* rdma_cm_id is the connection identifier (like socket) which is used
       * to define an RDMA connection.
       */
      if (rdma_create_id(cm_event_channel_, &cm_server_id_, NULL,
                         RDMA_PS_TCP)) {
        throw "rdma_create_id error!";
      }
      /* Explicit binding of rdma cm id to the socket credentials */
      if (rdma_bind_addr(cm_server_id_, (struct sockaddr *)&my_addr)) {
        throw "bind error!";
      }
      /* Now we start to listen on the passed IP and port. However unlike
       * normal TCP listen, this is a non-blocking call. When a new client is
       * connected, a new connection management (CM) event is generated on the
       * RDMA CM event channel from where the listening id was created. Here we
       * have only one channel, so it is easy. */
      /* backlog = 8 clients, same as TCP, see man listen*/
      if (rdma_listen(cm_server_id_, 8)) {
        throw "listen error!";
      }
    } catch (char const *s) {
      cout << s << endl;
      exit(-1);
    }
  }

  // Accept job and Run
  void WaitToRun() {
    struct rdma_cm_event *cm_event = nullptr;
    struct rdma_cm_id *cm_client_id = nullptr;
    int task_id = 0;
    while (true) {
      try {
        /* now, we expect a client to connect and generate a
         * RDMA_CM_EVNET_CONNECT_REQUEST We wait (block) on the connection
         * management event channel for the connect event.
         */
        if (process_rdma_cm_event(cm_event_channel_,
                                  RDMA_CM_EVENT_CONNECT_REQUEST, &cm_event)) {
          throw "process_rdma_cm_event error!";
        }
        /* Much like TCP connection, listening returns a new connection
         * identifier for newly connected client. In the case of RDMA, this is
         * stored in id field. For more details: man rdma_get_cm_event
         */
        cm_client_id = cm_event->id;
        if (!cm_client_id) throw "cm_client_id == nullptr!";
        /* now we acknowledge the event. Acknowledging the event free the
         * resources associated with the event structure. Hence any reference to
         * the event must be made before acknowledgment. Like, we have already
         * saved the client id from "id" field before acknowledging the event.
         */
        if (rdma_ack_cm_event(cm_event)) {
          throw "rdma_ack_cm_event error";
        }
        printf("A new RDMA client connection id is stored at %p\n",
              cm_client_id);
        pool_.enqueue(&RemoteCompactionServer::RunJob, this, cm_client_id,
                      task_id++);
      } catch (char const *s) {
        cout << s << endl;
      }
    }
  }

 private:
  // RunJob
  void RunJob(rdma_cm_id *client_cm_id, int task_id) {
    task_id %= 100;
    int thread_id = pool_.get_tid(std::this_thread::get_id());
    // Thread Start
    LOG(my_info, "thread %3d: [%d] Thread Start\n", thread_id, task_id);
    // Alloc RDMA Resources
    client_resources client_rc;
    memory_resources mem_rc;
    client_rc.id = client_cm_id;
    if (!setup_client_resources(client_rc)) {
      LOG(my_error, "setup_client_resources error!\n");
      return;
    }
    if (!accept_client_connection(cm_event_channel_, client_rc, mem_rc)) {
      LOG(my_error, "accept_client_connection error!\n");
      return;
    }
    if (!wait_receive_client_metadata(client_rc, mem_rc)) {
      LOG(my_error, "wait_receive_client_metadata error!\n");
      return;
    }
    void *buf = calloc(mem_rc.client_metadata_attr.length, 1);
    if (!regist_server_buffer(buf, client_rc, mem_rc)) {
      LOG(my_error, "regist_server_buffer error!\n");
      return;
    }

    string param;
    string result;
    string db_name;
    if (!receive_compaction_param(client_rc, mem_rc)) {
      LOG(my_error, "receive_compaction_param error!\n");
      return;
    }
    print_array_data(buf);
    size_t param_length = *(size_t *)buf;
    void *param_buf = (char *)buf + sizeof(size_t);

// Receive Param or CleanUp
#ifdef USE_SECONDARY_POOL
    if (!ReceiveParamOrCleanUp(param, db_name, client_cm_id, recv_buf,
                               MAX_BUF_SIZE, task_id,
                               secondary_dbs_[thread_id])) {
#else
    if (!ReceiveParamOrCleanUp(param, db_name, param_buf, param_length,
                               task_id)) {
#endif
      return;
    }
    // Do Compact
    std::this_thread::sleep_for(std::chrono::seconds(6));
    LOG(my_debug, "param=%s\n", param.c_str());
    char tmpres[20];
    sprintf(tmpres, "my_result_%d", task_id);
    result.assign(tmpres);
    LOG(my_info, "thread %3d: [%d] Finish compaction, Get result\n", thread_id,
        task_id);
    // Send Result
    SendResult(result, buf);
    print_array_data(buf);
    if (!send_compaction_result(client_rc, mem_rc)) {
      LOG(my_error, "send_compaction_result error!\n");
      return;
    }

    // #ifndef USE_SECONDARY_POOL
    //     delete secondary;  // Close current secondary
    //     secondary = nullptr;
    // #endif

    // if (!send_server_metadata_to_client(client_rc, mem_rc)) {
    //   LOG(my_error, "send_server_metadata_to_client error!\n");
    //   return;
    // }
    if (!disconnect_and_cleanup(cm_event_channel_, client_rc, mem_rc)) {
      LOG(my_error, "disconnect_and_cleanup error!\n");
      return;
    }

    // Thread End
    LOG(my_info, "thread %3d: [%d] Thread End\n", thread_id, task_id);
    free(buf);
  }

// Receive compactionParam or clean-up
#ifdef USE_SECONDARY_POOL
  bool ReceiveParamOrCleanUp(PluggableCompactionParam &param,
                             std::string &db_name, rdma_cm_id *client_cm_id,
                             void *recv_buf, size_t max_buf_size, int task_id,
                             std::unordered_map<string, DB *> &secondary_map) {
#else
  bool ReceiveParamOrCleanUp(string &param, std::string &db_name, void *buf,
                             size_t buf_length, int task_id) {
#endif
    string proto_param;
    // 1. receive Array from client
    // recv_string =

    // 2. Array -> ProtoParam
    // LOG(my_info, "[%d] Begin Array -> ProtoParam\n", task_id);
    char *recv_string_ptr = (char *)buf;
    recv_string_ptr[buf_length] = '\0';
    proto_param.assign(recv_string_ptr);
    // Get param
    ParseCompactionParamFromProto(param, proto_param);

    return true;
  }

  // Send Result
  static void SendResult(const string &result, void *send_buf) {
    // 1. Result -> ProtoResult
    string proto_result;
    SerializeCompactionResultToProto(proto_result, result);
    // 2. ProtoResult -> Array
    *(size_t *)send_buf = (size_t)proto_result.length();
    memcpy(send_buf + sizeof(size_t), proto_result.c_str(),
           proto_result.length() + 1);
    // 3. send Array to client
    ;

    // LOG(my_debug, "Send %ld bytes to client.\n", sent_size);
  }

#ifdef USE_SECONDARY_POOL
  // Protect code segment with mutex
  void WithMutex(std::function<void()> &&fn) {
    std::lock_guard<std::mutex> lock(mutex_);
    fn();
  }
#endif

#ifdef USE_SECONDARY_POOL
  std::vector<std::unordered_map<std::string, DB *>> secondary_dbs_{};
  std::mutex mutex_;
#endif
  struct sockaddr_in my_addr {};
  struct sockaddr_in remote_addr {};
  int port_;
  ThreadPool pool_;
  /* These are the RDMA resources needed to setup an RDMA connection */
  /* Event channel, where connection management (cm) related events are relayed
   */
  struct rdma_event_channel *cm_event_channel_ = nullptr;
  struct rdma_cm_id *cm_server_id_ = nullptr;
};

int main(int argc, char *argv[]) {
  uint thread_pool_size = std::thread::hardware_concurrency();
  int port = SERV_PORT;
  int log_level = 0;
  if (argc >= 2) thread_pool_size = std::stoi(argv[1]);
  if (argc >= 3) port = std::stoi(argv[2]);
  if (argc >= 4) log_level = std::stoi(argv[3]);
  if (log_level >= 1) my_debug = true;
  if (log_level >= 2) my_info = true;
  cout << "thread_pool_size set to " << thread_pool_size << endl;
  cout << "port set to " << port << endl;
  cout << "log_level set to " << log_level << endl;

  RemoteCompactionServer server(thread_pool_size, port);

  server.SetupServer();
  server.WaitToRun();

  return 0;
}
