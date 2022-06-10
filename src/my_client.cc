/*
 * An example RDMA client side code.
 * Author: Animesh Trivedi
 *         atrivedi@apache.org
 */

#include "my_common.h"

/* These are basic RDMA resources */
/* These are RDMA connection related resources */
// static struct rdma_event_channel *cm_event_channel = NULL;
// static struct rdma_cm_id *cm_client_id = NULL;
// static struct ibv_pd *pd = NULL;
// static struct ibv_comp_channel *io_completion_channel = NULL;
// static struct ibv_cq *client_cq = NULL;
// static struct ibv_qp_init_attr qp_init_attr;
// static struct ibv_qp *client_qp;
// /* These are memory buffers related resources */
// static struct ibv_mr *client_metadata_mr = NULL, *client_buffer_mr = NULL;
// static struct rdma_buffer_attr client_metadata_attr;
// static struct ibv_send_wr client_send_wr, *bad_client_send_wr = NULL;
// static struct ibv_sge client_send_sge;

struct client_resources {
  rdma_event_channel *cm_event_channel = nullptr;
  rdma_cm_id *cm_client_id = nullptr;
  ibv_pd *pd = nullptr;
  ibv_comp_channel *io_completion_channel = nullptr;
  ibv_cq *client_cq = nullptr;
  ibv_qp_init_attr qp_init_attr{};
  ibv_qp *client_qp = nullptr;
};

struct memory_resources {
  ibv_mr *client_metadata_mr = nullptr, *client_buffer_mr = nullptr;
  rdma_buffer_attr client_metadata_attr{};
  ibv_send_wr client_send_wr, *bad_client_send_wr = nullptr;
  ibv_sge client_send_sge{};
};
/* Source and Destination buffers, where RDMA operations source and sink */
// static char *src = NULL, *dst = NULL;
static char *buf = NULL;

static void print_array_data(void *buffer) {
  unsigned long result_length = *(unsigned long *)buffer;
  char *result_data = (char *)buffer + sizeof(unsigned long);
  result_data[result_length] = '\0';
  printf("\nBuffer length:%lu, data:%s\n", result_length, result_data);
}

/* This function prepares client side connection resources for an RDMA
 * connection */
static int client_prepare_connection(client_resources &client_rc,
                                     struct sockaddr_in *s_addr) {
  struct rdma_cm_event *cm_event = NULL;
  int ret = -1;
  /*  Open a channel used to report asynchronous communication event */
  client_rc.cm_event_channel = rdma_create_event_channel();
  if (!client_rc.cm_event_channel) {
    printf("Creating cm event channel failed, errno: %d \n", -errno);
    return -errno;
  }
  /* rdma_cm_id is the connection identifier (like socket) which is used
   * to define an RDMA connection.
   */
  ret = rdma_create_id(client_rc.cm_event_channel, &client_rc.cm_client_id,
                       NULL, RDMA_PS_TCP);
  if (ret) {
    printf("Creating cm id failed with errno: %d \n", -errno);
    return -errno;
  }
  /* Resolve destination and optional source addresses from IP addresses  to
   * an RDMA address.  If successful, the specified rdma_cm_id will be bound
   * to a local device. */
  ret = rdma_resolve_addr(client_rc.cm_client_id, NULL,
                          (struct sockaddr *)s_addr, 3000);
  if (ret) {
    printf("Failed to resolve address, errno: %d \n", -errno);
    return -errno;
  }
  ret = process_rdma_cm_event(client_rc.cm_event_channel,
                              RDMA_CM_EVENT_ADDR_RESOLVED, &cm_event);
  if (ret) {
    printf("Failed to receive a valid event RDMA_CM_EVENT_ADDR_RESOLVED, ret = %d \n", ret);
    return ret;
  }
  /* we ack the event */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    printf("Failed to acknowledge the CM event, errno: %d\n", -errno);
    return -errno;
  }

  /* Resolves an RDMA route to the destination address in order to
   * establish a connection */
  ret = rdma_resolve_route(client_rc.cm_client_id, 3000);
  if (ret) {
    printf("Failed to resolve route, erno: %d \n", -errno);
    return -errno;
  }
  ret = process_rdma_cm_event(client_rc.cm_event_channel,
                              RDMA_CM_EVENT_ROUTE_RESOLVED, &cm_event);
  if (ret) {
    printf("Failed to receive a valid event RDMA_CM_EVENT_ROUTE_RESOLVED, ret = %d \n", ret);
    return ret;
  }
  /* we ack the event */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    printf("Failed to acknowledge the CM event, errno: %d \n", -errno);
    return -errno;
  }
  /* Protection Domain (PD) is similar to a "process abstraction"
   * in the operating system. All resources are tied to a particular PD.
   * And accessing recourses across PD will result in a protection fault.
   */
  client_rc.pd = ibv_alloc_pd(client_rc.cm_client_id->verbs);
  if (!client_rc.pd) {
    printf("Failed to alloc pd, errno: %d \n", -errno);
    return -errno;
  }
  printf("ibv_alloc_pd success!\n");
  
  /* Now we need a completion channel, were the I/O completion
   * notifications are sent. Remember, this is different from connection
   * management (CM) event notifications.
   * A completion channel is also tied to an RDMA device, hence we will
   * use cm_client_id->verbs.
   */
  client_rc.io_completion_channel =
      ibv_create_comp_channel(client_rc.cm_client_id->verbs);
  if (!client_rc.io_completion_channel) {
    printf("Failed to create IO completion event channel, errno: %d\n", -errno);
    return -errno;
  }
  /* Now we create a completion queue (CQ) where actual I/O
   * completion metadata is placed. The metadata is packed into a structure
   * called struct ibv_wc (wc = work completion). ibv_wc has detailed
   * information about the work completion. An I/O request in RDMA world
   * is called "work" ;)
   */
  client_rc.client_cq = ibv_create_cq(
      client_rc.cm_client_id->verbs /* which device*/,
      CQ_CAPACITY /* maximum capacity*/, NULL /* user context, not used here */,
      client_rc.io_completion_channel /* which IO completion channel */,
      0 /* signaling vector, not used here*/);
  if (!client_rc.client_cq) {
    printf("Failed to create CQ, errno: %d \n", -errno);
    return -errno;
  }
  ret = ibv_req_notify_cq(client_rc.client_cq, 0);
  if (ret) {
    printf("Failed to request notifications, errno: %d\n", -errno);
    return -errno;
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
      client_rc.client_cq; /* Where should I notify for receive completion
                              operations */
  client_rc.qp_init_attr.send_cq =
      client_rc
          .client_cq; /* Where should I notify for send completion operations */
  /*Lets create a QP */
  ret = rdma_create_qp(client_rc.cm_client_id /* which connection id */,
                       client_rc.pd /* which protection domain*/,
                       &client_rc.qp_init_attr /* Initial attributes */);
  if (ret) {
    printf("Failed to create QP, errno: %d \n", -errno);
    return -errno;
  }
  client_rc.client_qp = client_rc.cm_client_id->qp;
  return 0;
}

/* Connects to the RDMA server */
static int client_connect_to_server(client_resources &client_rc) {
  struct rdma_conn_param conn_param;
  struct rdma_cm_event *cm_event = NULL;
  int ret = -1;
  bzero(&conn_param, sizeof(conn_param));
  conn_param.initiator_depth = CONN_DEPTH;
  conn_param.responder_resources = CONN_DEPTH;
  conn_param.retry_count = 3;  // if fail, then how many times to retry
  ret = rdma_connect(client_rc.cm_client_id, &conn_param);
  if (ret) {
    printf("Failed to connect to remote host , errno: %d\n", -errno);
    return -errno;
  }
  ret = process_rdma_cm_event(client_rc.cm_event_channel,
                              RDMA_CM_EVENT_ESTABLISHED, &cm_event);
  if (ret) {
    printf("Failed to get cm event RDMA_CM_EVENT_ESTABLISHED, ret = %d \n", ret);
    return ret;
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    printf("Failed to acknowledge cm event, errno: %d\n", -errno);
    return -errno;
  }
  return 0;
}

static int regist_buffer(void *buffer, size_t max_size,
                         client_resources &client_rc,
                         memory_resources &mem_rc) {
  /* We need to setup requested memory buffer. This is where the client will
   * do RDMA READs and WRITEs. */
  mem_rc.client_buffer_mr = rdma_buffer_register(
      client_rc.pd /* which protection domain */, buffer,
      (uint32_t)max_size /* what size to allocate */,
      (ibv_access_flags)(IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
  if (!mem_rc.client_buffer_mr) {
    printf("Server failed to create a buffer \n");
    /* we assume that it is due to out of memory error */
    return -ENOMEM;
  }
  return 0;
}

/* Exchange buffer metadata with the server. The client sends its, and then
 * receives from the server. The client-side metadata on the server is _not_
 * used because this program is client driven. But it shown here how to do it
 * for the illustration purposes
 */
static int client_send_metadata_to_server(client_resources &client_rc,
                                          memory_resources &mem_rc) {
  struct ibv_wc wc;
  int ret = -1;
  /* we prepare metadata for the buffer */
  mem_rc.client_metadata_attr.address = (uint64_t)mem_rc.client_buffer_mr->addr;
  mem_rc.client_metadata_attr.length = mem_rc.client_buffer_mr->length;
  mem_rc.client_metadata_attr.stag.local_stag = mem_rc.client_buffer_mr->lkey;
  /* now we register the metadata memory */
  mem_rc.client_metadata_mr = rdma_buffer_register(
      client_rc.pd, &mem_rc.client_metadata_attr,
      sizeof(mem_rc.client_metadata_attr), IBV_ACCESS_LOCAL_WRITE);
  if (!mem_rc.client_metadata_mr) {
    printf("Failed to register the client metadata buffer, ret = %d \n", ret);
    return ret;
  }
  /* now we fill up SGE */
  mem_rc.client_send_sge.addr = (uint64_t)mem_rc.client_metadata_mr->addr;
  mem_rc.client_send_sge.length = (uint32_t)mem_rc.client_metadata_mr->length;
  mem_rc.client_send_sge.lkey = mem_rc.client_metadata_mr->lkey;
  /* now we link to the send work request */
  bzero(&mem_rc.client_send_wr, sizeof(mem_rc.client_send_wr));
  mem_rc.client_send_wr.sg_list = &mem_rc.client_send_sge;
  mem_rc.client_send_wr.num_sge = 1;
  mem_rc.client_send_wr.opcode = IBV_WR_SEND;
  mem_rc.client_send_wr.send_flags = IBV_SEND_SIGNALED;
  /* Now we post it */
  ret = ibv_post_send(client_rc.client_qp, &mem_rc.client_send_wr,
                      &mem_rc.bad_client_send_wr);
  if (ret) {
    printf("Failed to send client metadata, errno: %d \n", -errno);
    return -errno;
  }
  /* at this point we are expecting 2 work completion. One for our
   * send and one for recv that we will get from the server for
   * its buffer information */
  ret = process_work_completion_events(client_rc.io_completion_channel, &wc, 1);
  if (ret != 1) {
    printf("We failed to send client metadata , ret = %d \n", ret);
    return ret;
  }
  return 0;
}

/* This function disconnects the RDMA connection from the server and cleans up
 * all the resources.
 */
static void client_disconnect_and_clean(client_resources &client_rc,
                                        memory_resources &mem_rc) {
  struct rdma_cm_event *cm_event = NULL;
  int ret;
  ret = process_rdma_cm_event(client_rc.cm_event_channel,
                              RDMA_CM_EVENT_DISCONNECTED, &cm_event);
  if (ret) {
    printf("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n", ret);
    // continuing anyways
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    printf("Failed to acknowledge cm event, errno: %d\n", -errno);
    // continuing anyways
  }
  /* Destroy QP */
  rdma_destroy_qp(client_rc.cm_client_id);
  /* Destroy client cm id */
  ret = rdma_destroy_id(client_rc.cm_client_id);
  if (ret) {
    printf("Failed to destroy client id cleanly, %d \n", -errno);
    // we continue anyways;
  }
  /* Destroy CQ */
  ret = ibv_destroy_cq(client_rc.client_cq);
  if (ret) {
    printf("Failed to destroy completion queue cleanly, %d \n", -errno);
    // we continue anyways;
  }
  /* Destroy completion channel */
  ret = ibv_destroy_comp_channel(client_rc.io_completion_channel);
  if (ret) {
    printf("Failed to destroy completion channel cleanly, %d \n", -errno);
    // we continue anyways;
  }
  /* Destroy memory buffers */
  rdma_buffer_deregister(mem_rc.client_metadata_mr);
  rdma_buffer_deregister(mem_rc.client_buffer_mr);
  /* Destroy protection domain */
  ret = ibv_dealloc_pd(client_rc.pd);
  if (ret) {
    printf("Failed to destroy client protection domain cleanly, %d \n", -errno);
    // we continue anyways;
  }
  rdma_destroy_event_channel(client_rc.cm_event_channel);
}

void usage() {
  printf("Usage:\n");
  printf(
      "rdma_client: [-a <server_addr>] [-p <server_port>] -s string "
      "(required)\n");
  printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
  exit(1);
}

int main(int argc, char **argv) {
  struct sockaddr_in server_sockaddr;
  int ret, option;
  bzero(&server_sockaddr, sizeof server_sockaddr);
  server_sockaddr.sin_family = AF_INET;
  server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  /* buffers are NULL */
  client_resources client_rc;
  memory_resources mem_rc;
  buf = (char *)calloc(64 * 1024, 1);
  *(unsigned long *)buf = 8;
  strncpy(buf + sizeof(unsigned long), "my_param", 8);
  /* Parse Command Line Arguments */
  while ((option = getopt(argc, argv, "a:p:")) != -1) {
    switch (option) {
      case 'a':
        /* remember, this overwrites the port info */
        ret = get_addr(optarg, (struct sockaddr *)&server_sockaddr);
        if (ret) {
          printf("Invalid IP \n");
          return ret;
        }
        break;
      case 'p':
        /* passed port to listen on */
        server_sockaddr.sin_port = htons(strtol(optarg, NULL, 0));
        break;
      default:
        usage();
        break;
    }
  }
  if (!server_sockaddr.sin_port) {
    /* no port provided, use the default port */
    server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
  }
  ret = client_prepare_connection(client_rc, &server_sockaddr);
  if (ret) {
    printf("Failed to setup client connection , ret = %d \n", ret);
    return ret;
  }
  ret = client_connect_to_server(client_rc);
  if (ret) {
    printf("Failed to setup client connection , ret = %d \n", ret);
    return ret;
  }
  ret = regist_buffer(buf, 64 * 1024, client_rc, mem_rc);
  if (ret) {
    printf("Failed to regist_buffer , ret = %d \n", ret);
    return ret;
  }
  ret = client_send_metadata_to_server(client_rc, mem_rc);
  if (ret) {
    printf("Failed to send metadata to server , ret = %d \n", ret);
    return ret;
  }

  client_disconnect_and_clean(client_rc, mem_rc);

  print_array_data(buf);
  /* We free the buffers */
  free(buf);
  return ret;
}
