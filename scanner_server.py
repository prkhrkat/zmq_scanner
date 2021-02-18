
import threading
import zmq
import time
import scan as scan
import base64

CONNECTION_URL = 'tcp://*:5558'

class Server(threading.Thread):
    def __init__(self):
        self._stop = threading.Event()
        threading.Thread.__init__(self)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind('CONNECTION_URL')

        backend = context.socket(zmq.DEALER)
        backend.bind('inproc://backend_endpoint')

        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend,  zmq.POLLIN)

        while not self.stopped():
            sockets = dict(poll.poll())
            if frontend in sockets:
                if sockets[frontend] == zmq.POLLIN:
                    _id = frontend.recv()
                    json_msg = frontend.recv_json()

                    handler = RequestHandler(context, _id, json_msg)
                    handler.start()

            if backend in sockets:
                if sockets[backend] == zmq.POLLIN:
                    _id = backend.recv()
                    msg = backend.recv()
                    frontend.send(_id, zmq.SNDMORE)
                    frontend.send(msg)

        frontend.close()
        backend.close()
        context.term()


class RequestHandler(threading.Thread):
    def __init__(self, context, id, msg):

        """
        RequestHandler
        :param context: ZeroMQ context
        :param id: Requires the identity frame to include in the reply so that it will be properly routed
        :param msg: Message payload for the worker to process
        """
        threading.Thread.__init__(self)
        print("--------------------Entered requesthandler--------------------")
        self.context = context
        self.msg = msg
        self._id = id

    def process(self, obj):
        print("Start of Processing", time.asctime())
        imgstr = obj['payload']

        # img = Image.open(BytesIO(b64decode(imgstr)))
        if obj['type'] == 'corner_det':
            doc_corners = scan.main_scan_corners(imgstr)
            return_dict = {}
            return_dict["preds"] = doc_corners.tolist()
            return return_dict
        if obj['type'] == 'transform':
            corners = obj['corners']
            image_bytes = scan.main_scan(corners, imgstr)
            encoded = base64.b64encode(image_bytes).decode("utf-8")
            print(type(encoded))
            return_dict = {}
            return_dict["preds"] = encoded
            return return_dict

    def run(self):
        # Worker will process the task and then send the reply back to the DEALER backend socket via inproc
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend_endpoint')
        #print('Request handler started to process %s\n' % self.msg)

        # Simulate a long-running operation
        output = self.process(self.msg)

        worker.send(self._id, zmq.SNDMORE)
        worker.send_json(output)
        del self.msg

        print('Request handler quitting.\n')
        worker.close()


def main():
    # Start the server that will handle incoming requests
    print("Ready for Server Start", time.asctime())
    server = Server()
    server.start()
    print("Server started", time.asctime())


if __name__ == '__main__':
    main()
