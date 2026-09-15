/*
 * Copyright (c) 2018, AT&T Intellectual Property. All rights reserved.
 * Copyright (c) 2017 by Brocade Communications Systems, Inc.
 * All rights reserved.
 *
 * SPDX-License-Identifier: MPL-2.0
 */

#include <linux/netlink.h>
#include <linux/rtnetlink.h>

#include <libmnl/libmnl.h>
#include <czmq.h>
#include <stdbool.h>
#include <zmq.h>

#include "broker.h"
#include "route_broker_internal.h"

static pthread_t broker_consumer_thread;
static volatile bool broker_consumer_stop;

static object_broker_client_publish_cb obj_kernel_publish;

static void *broker_consumer(void *arg)
{
	struct route_broker_client *client;
	struct broker_client *bc;
	void *obj;

	client = route_broker_client_create("kernel");

	while (!broker_consumer_stop) {
		while ((obj = route_broker_client_get_data(client, &bc))) {
			if (obj_kernel_publish(obj, NULL)) {
				client->errors++;
				broker_log_err("publish %s: "
					       "consumed %" PRIu64
					       " behind %" PRIu64
					       " errno (%d) %s\n",
					       bc->name, bc->consumed,
					       bc->broker->id -
					       bc->broker_obj.id,
					       errno, strerror(errno));
			} else if (broker_is_log_detail()) {
				broker_log_debug("publish %s: "
						 "consumed %" PRIu64
						 " behind %" PRIu64 "\n",
						 bc->name, bc->consumed,
						 bc->broker->id -
						 bc->broker_obj.id);
			}

			route_broker_client_free_data(client, obj);
		}
	}

	route_broker_client_delete(client);
	return NULL;
}

int route_broker_kernel_init(object_broker_client_publish_cb publish)
{
	int rc;

	obj_kernel_publish = publish;
	rc = pthread_create(&broker_consumer_thread, NULL,
			    broker_consumer, NULL);
	return rc;

}

/*
 * Ask the consumer to stop rather than cancelling it.
 *
 * This used to be pthread_cancel() followed by pthread_join(), and it is
 * called on every FPM session teardown -- not at process exit. The consumer
 * blocks in route_broker_client_get_data(), which waits on a condition
 * variable: a cancellation point that reacquires route_broker_mutex before it
 * returns. There is no pthread_cleanup handler anywhere in this library, so a
 * cancel could destroy the thread while it held that mutex, between taking an
 * object and freeing it, or inside a ZMQ send.
 *
 * The process then carried on with a mutex that would never be released and
 * ZMQ state torn half-way, and the next publish faulted inside the allocator.
 * brokerd died on every single FPM bounce -- twenty-one cores in one session,
 * no exceptions -- and took the data plane with it each time, because the data
 * plane is restarted when its feed dies.
 *
 * The loop's wait times out after a second, so the flag is noticed within
 * that and the thread leaves by its own route with its locks released and its
 * client deleted.
 */
void route_broker_kernel_shutdown(void)
{
	broker_consumer_stop = true;
	pthread_join(broker_consumer_thread, NULL);
}
