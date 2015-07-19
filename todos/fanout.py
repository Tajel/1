from threading import Thread
from Queue import Queue, Empty

import pgpubsub


SUBSCRIBE = 0
UNSUBSCRIBE = 1
EXIT = 2

# We want to spend most of the time listening for new postgres messages.  We'll
# do that in 100ms chunks.  In between each of those chunks, we'll check for any
# new subscribers.
PG_BLOCK_TIME = 0.1


def subscribe(q_in, db_params, channels):
    ps = pgpubsub.connect(**db_params)
    for c in channels:
        ps.listen(c)

    # Each key in this dict should be the id() of the Queue object that we're
    # posting to.  Each value should be a tuple of (q_object, filter_callback)
    subscribers = {}

    while True:
        # check q_in for any new subscribers.
        try:
            cmd = q_in.get_nowait()
            if cmd[0] == SUBSCRIBE:
                q = cmd[1]
                should_send = cmd[2]
                subscribers[id(q)] = (q, should_send)
            elif cmd[0] == UNSUBSCRIBE:
                q = cmd[1]
                subscribers.pop(id(q), None)
            elif cmd[0] == EXIT:
                ps.close()
                for q, _ in subscribers.values():
                    # Sending a None down a subscriber's queue will tell it that
                    # we're exiting.
                    q.put(None)
                break
        except Empty:
            pass

        # check the pubsub for any new messages.
        msgs = ps.get_events(select_timeout=PG_BLOCK_TIME)
        if msgs:
            for msg in msgs:
                for q, should_send in subscribers.values():
                    if should_send(msg):
                        q.put(msg)
    del subscribers


class PubSubFanout(object):
    """
    Launches a background thread that subscribes to a Postgres pubsub, and
    forwards messages to an arbitrary number of in-process queues.  Can be used
    to feed a bunch of websockets with just one DB connection.
    """
    def __init__(self, channels, **db_params):
        self.db_params = db_params
        self.q_in = Queue()
        self.worker = Thread(target=subscribe, args=(
            self.q_in,
            self.db_params,
            channels,
        ))
        self.worker.start()

    def subscribe(self, should_send=lambda x: True):
        q = Queue()
        self.q_in.put((SUBSCRIBE, q, should_send))
        return q

    def unsubscribe(self, q):
        self.q_in.put((UNSUBSCRIBE, q))

    def close(self):
        self.q_in.put((EXIT,))
