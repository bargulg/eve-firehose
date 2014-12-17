#!/usr/bin/env python2
import sys
import zmq
import zlib
import signal
import simplejson
import multiprocessing

import marketmessageprocessor


# needed for initializing worker processes
processor = None

# general runtime statistics used by the callback method
stats = {"failed": 0, "orders": 0, "history": 0, "errors": {}}

# worker process pool
pool = None


def init_worker():
    # ignore keyboard interrupt
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # create a process-local instance of MarketMessageProcessor
    global processor
    processor = marketmessageprocessor.MarketMessageProcessor()


def process_message(payload):
    # decompress and parse message
    message = zlib.decompress(payload)
    message = simplejson.loads(message)

    if message["resultType"] == "orders":
        return processor.process_orders(message)
    elif message["resultType"] == "history":
        #return processor.process_history(message)
        return {"success": True, "type": "history", "number": 1}
    else:
        return {"success": False, "type": message["resultType"]}


def callback(result):
    # just displays runtime stats
    if not result["success"]:
        stats["failed"] += 1
        if result["reason"] not in stats["errors"]:
            stats["errors"][result["reason"]] = 0
        stats["errors"][result["reason"]] += 1
    elif result["type"] == "orders":
        stats["orders"] += result["number"]
    elif result["type"] == "history":
        stats["history"] += result["number"]

    sys.stdout.write("\r\t" + str(stats["orders"]) + " orders / " + str(stats["history"])
                     + " history / " + str(stats["failed"]) + " failed ")
    sys.stdout.flush()


def main():
    # zmq init
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect('tcp://relay-eu-germany-1.eve-emdr.com:8050')
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    # process pool init
    global pool
    pool = multiprocessing.Pool(initializer=init_worker, initargs=[])

    while True:
        # read a message and outsource it to process pool
        raw_data = subscriber.recv()
        pool.apply_async(func=process_message, args=[raw_data], callback=callback)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        # wait for pool to finish processing queued work and clean it up
        pool.close()
        pool.join()

        # fix term
        print

        # show failure types and counts
        print
        print "Failures by type:"
        for failType in sorted(stats["errors"], key=stats["errors"].get, reverse=True):
            print "\t", stats["errors"][failType], "\t", failType
