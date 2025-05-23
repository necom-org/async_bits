#from ast import keyword
import zmq
import pickle
from threading import Timer
import time
from time import sleep
import numpy as np
from copy import deepcopy
import traceback

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
    def stop(self):
        self.cancel()

# What about one that reads a list of tLs and other values? Potentially with metadata too, under 'meta'?
# The pattern should hold. Just need to look out for changing parameters- perhaps can be handled by exception? In that case, we'll probably want to return just the good data, and perhaps save the new (different parameter) data for the next round? Or maybe easier to just miss one packet of it.
class ZMQSubReadData: # Reads a list of tL and dataLs
    def __init__(self, port, topic, hwm=20):
        self.Nsent = 0
        self.port = port
        self.topic = topic
        self.socket = zmq.Context().socket(zmq.SUB)
        self.socket.set_hwm(hwm)
        self.socket.connect("tcp://localhost:%s" % self.port)
        self.socket.setsockopt(zmq.SUBSCRIBE, bytes(topic, "utf"))
        self.tLast = time.time()

    def retrieve(self): # What assumptions can be made here??

        if self.socket.poll(10):
            topic, payload= self.socket.recv().split(b' ', 1)
            payloadD = pickle.loads(payload)
            self.tLast = time.time()
            return payloadD

    def close(self):
        self.socket.close()


def appendListsInDict(d1, d2):
    d1 = deepcopy(d1)
    if not d1.keys() == d2.keys():
        #print(f"keys don't match: {d1.keys()} vs {d2.keys()}")
        return None

    else:
        for key in d1:
            newL = d2[key]
            if not np.iterable(newL):
                newL = [newL]
            d1[key].extend(newL)

    return d1



class ZMQSubReadChunked:
    """Reads chunkcs of data in dict form, e.g.

        * datD: ditc-like(
            "name1": [],
            "name2": [],
            "name3": [],
            ...
        * metaData = {}

    Will assemble into longer arrays/chunks for analysis

    It should assemble incoming arrays into a single dictionary with longer arrays.

    It operates on dictionaries of lists.

    If the current datD is None, it is set as the first incoming datD.
    Incoming datD's are 'appended' to it.
    If the newer dictionary has a different number of parameters (more or less), the merge should fail.
    In this case, we should save the newer datD to be used as the starting dict for the next retrieval.
    And the old dict should be returned (even though it'll probably have too few params)

    Maaaybe should check timstamps for continuity to be sure comms are ok

    Note 1: Most code here currently is about sticking data into this dict-of-lists structure. To get the required
    number of data points, we end up doing quite a lot of blocking.
        Maybe much of this code could be replaced by a accumulating daemon, which retrieves the data when it's
    available and sticks it on a queue. The "retrieve" method would then return immediately if sufficient data isn't available.

    """

    def __init__(self, port, topic, metaData = {}, hwm=20):
        self.Nsent = 0
        self.port = port
        self.topic = topic

        self.socket = zmq.Context().socket(zmq.SUB)
        self.socket.set_hwm(hwm)
        self.socket.connect("tcp://localhost:%s" % self.port)
        self.socket.setsockopt(zmq.SUBSCRIBE, bytes(topic, "utf"))
        self.tLast = time.time()
        self.leftover_datD = {}
        self.metaData = metaData


    def retrieve(self, Nmin=0): # What assumptions can be made here??
        #print("enter retrieve")
        datD = self.leftover_datD # leftover or None
        self.leftover_datD = {}
        k = 0
        bStop = False
        try:
            Nread = len(datD[list(datD.keys())[0]]) if datD else 0
            while not bStop:
                k+=1
                if k > 30:
                    #print(f"too many fails, returning: {datD}")
                    #return datD, len(datD[list(datD.keys())[0]])
                    break

                while self.socket.poll(20):
                    #print('.', end='')
                    topic, payload= self.socket.recv().split(b' ', 1)
                    payloadD = pickle.loads(payload)
                    #print(payloadD.keys())

                    appendedD = appendListsInDict(datD, payloadD['datD'])
                    if appendedD is None:
                        # there was a mismatch between new and prev dicts,
                        # dump the old one and start again
                        self.leftover_datD = {key:list(val) for key, val in payloadD['datD'].items()}
                        bStop = True
                        break;

                    datD = appendedD
                    # Will assume they're all the same lenght for now
                    Nread = len(datD[list(datD.keys())[0]])
                    if "metaData" in payloadD:
                        self.metaData = payloadD['metaData']

                    if Nread >= Nmin:
                        break;
        except Exception as e:
            print("exception in retrieval:")
            print(e)


        print(f"retrieved {Nread} values")
        self.Nsent += Nread
        tElapsed = time.time()-self.tLast
        if tElapsed > 10:
            print(f"data retrieval rate: {self.Nsent/tElapsed}")
            self.Nsent -= self.Nsent/4
            self.tLast += tElapsed/4

        #print("finished retrieve")
        if Nread > 0:
            return {'data': datD, 'metaData': self.metaData, 'Nread': Nread}
        else:
            return None

    def clear_waiting(self):
        n_cleared = 0
        while self.socket.poll(1):
            self.socket.recv()
            n_cleared += 1
        return n_cleared

    def close(self):
        self.socket.close()

class ZMQPubSendData:
    def __init__(self, port, topic, hwm=20):
        self.port = port
        try: # make sure topic is bytes
            self.topic = bytes(topic, 'utf')

        except TypeError:
            self.topic = topic

        self.socket = zmq.Context().socket(zmq.PUB)
        self.socket.set_hwm(hwm)

        self.socket.bind("tcp://*:%s" % self.port)
        #self.socket.connect("tcp://localhost:%s" % self.port)

    def send(self, **kwargs):#tL, dataL):
        #self.socket.send(self.topic + b" " + pickle.dumps( {'tL': tL, 'dataL': dataL}))
        self.socket.send(self.topic + b" " + pickle.dumps( kwargs ))

class ZMQChunkedPublisher:
    """ A ZMQ sender for sending clusters of data at a time

    syntax:
        sender.send

    Sent data will be of the to form:
        * datD: ditc-like(
            "name1": [],
            "name2": [],
            "name3": [],
            "name3": [],
            ...
        * metaData = {}
        )
        * Objects are expected to be lists


    """
    def __init__(self, port, topic, metaData={}, hwm=20):
        self.metaData = metaData
        self.port = port
        try: # make sure topic is bytes
            self.topic = bytes(topic, 'utf')

        except TypeError:
            self.topic = topic

        self.socket = zmq.Context().socket(zmq.PUB)
        self.socket.set_hwm(hwm)

        self.socket.bind("tcp://*:%s" % self.port)
        #self.socket.connect("tcp://localhost:%s" % self.port)

    def setMetaData(self, metaData):
        self.metaData = metaData

    def send(self, **kwargs):
        datD = dict(**kwargs)
        self.socket.send(self.topic + b" " + pickle.dumps( {'datD':datD, 'metaData': self.metaData}))

from DPM import DockPlotManager
import pyqtgraph as pg
class SimpleSourcePlotter:
    # defaultPlotKwargs {mode : 1} is to set appendMode as True. This is clunky and DPM should be changed
    @staticmethod
    def defaultPreProcess(data):
        #print(f"addToDPM recieved {N}, keys: {data}")
        #print(f"plotting: {data}")
        x = None
        if "tL" in data:
            x = data.pop('tL')
        elif "t" in data:
            x = data.pop('t')
        elif "x" in data:
            x = data.pop('x')

        if x is not None:
            return {key:{'x':x, 'y': val} for key,val in data.items()}
        else:

            return data

    def __init__(self, inputF, label, preProcessF = None, defaultPlotKwargs = dict(mode = 0), poll_interval = 0.2):

        self.inputF = inputF
        self.preProcessF = self.defaultPreProcess if preProcessF is None else preProcessF
        self.dpm = DockPlotManager(f"{label} Plot", defaultPlotKwargs = defaultPlotKwargs)
        #This should really be handled by DPM itself, but isn't currently
        def addToPlot(data):
            for key in data:
                self.dpm.addData(key, data[key])

        self.addToPlot = addToPlot
        self.poll_interval = poll_interval

    def update(self):
        try:
            new_data = self.inputF();
            if new_data is not None:
                if self.preProcessF is not None:
                    new_data = self.preProcessF(new_data)
                self.addToPlot(new_data)
        except Exception as e:
            print("exception in plotter:")
            print(e)
            track = traceback.format_exc()
            print(track)

    def start(self):
        timer = pg.QtCore.QTimer()
        #timer = RepeatTimer(self.run_interval, self.update)#pg.QtCore.QTimer()
        timer.timeout.connect(self.update)
        timer.start( int(self.poll_interval*1000) )
        self.timer = timer

    def stop(self):
        if self.timer:
            self.timer.stop()
        else:
            raise Exception("Can't stop SimpleSourcePlotter (wasn't started)")

class ChunkedSourcePlotter:
    # defaultPlotKwargs {mode : 1} is to set appendMode as True. This is clunky and DPM should be changed
    @staticmethod
    def defaultPreProcess(recieved):
        N = recieved['Nread']
        data = recieved['data']
        metaData = recieved['metaData']
        x = None
        if "tL" in data:
            x = data.pop('tL')
        elif "t" in data:
            x = data.pop('t')
        elif "x" in data:
            x = data.pop('x')

        if x is not None:
            to_plot = {key:{'x':x, 'y': val} for key,val in data.items()}
            #print(to_plot)
            return to_plot
        else:

            return data

    def __init__(self, inputF, label, preProcessF = None, defaultPlotKwargs = dict(mode = 1), poll_interval = 0.2):

        self.inputF = inputF
        self.preProcessF = self.defaultPreProcess if preProcessF is None else preProcessF
        self.dpm = DockPlotManager(f"{label} Plot", defaultPlotKwargs = defaultPlotKwargs)
        #This should really be handled by DPM itself, but isn't currently
        def addToPlot(data):
            for key in data:
                print(f"adding: {key}")
                self.dpm.addData(key, data[key])

        self.addToPlot = addToPlot
        self.poll_interval = poll_interval

    def update(self):
        try:
            new_data = self.inputF();
            if new_data is not None:
                if self.preProcessF is not None:
                    new_data = self.preProcessF(new_data)
                self.addToPlot(new_data)
        except Exception as e:
            print("exception in plotter:")
            print(e)
            track = traceback.format_exc()
            print(track)

    def start(self):
        timer = pg.QtCore.QTimer()
        #timer = RepeatTimer(self.run_interval, self.update)#pg.QtCore.QTimer()
        timer.timeout.connect(self.update)
        timer.start( int(self.poll_interval*1000) )
        self.timer = timer

    def stop(self):
        if self.timer:
            self.timer.stop()
        else:
            raise Exception("Can't stop ChunkedPlotter (wasn't started)")

class AsyncTransformer:

    def __init__(self, inputF, transformF, outputF, state_updateF = None, state = None, run_interval = 0.2):
        self.inputF = inputF
        self.transformF = transformF
        self.outputF = outputF
        self.state_updateF = state_updateF
        self.state = state
        self.run_interval = run_interval

    # Placeholder- maybe not needed
    def inittialise(self):
        pass

    # Placeholder- maybe not needed
    def close(self):
        pass


    def update(self):
        if 1:
            try:
                new_data = self.inputF();
                #print(f"new_data: {new_data}")
                if new_data:
                    if self.state_updateF is not None:
                        self.state = self.state_updateF(new_data, state=self.state )
                        transformed_data = self.transformF(new_data, state = self.state);
                    else:
                        transformed_data = self.transformF(new_data);
                    if transformed_data:
                        self.outputF(transformed_data)
            except Exception as e:
                print("exception!")
                print(e)
                track = traceback.format_exc()
                print(track)

    def start(self):
        timer = RepeatTimer(self.run_interval, self.update)#pg.QtCore.QTimer()
        #timer.timeout.connect(self.update)
        timer.start()
        self.timer = timer

    def stop(self):
        self.timer.stop()

if __name__ == "__main__":

    from numpy import random, arange
    app = pg.mkQApp("Plotting Example")
    def test_ZMQ_chunked_pub():
        port = 5000

        tNow = 0
        dt = .2
        def generate_data():
            nonlocal tNow
            Nsamps = int(5 + 5*(np.random.uniform()**2))
            tA = tNow + dt*np.arange(Nsamps)
            y1 = 2*np.sin(5*tA) + .1*np.random.normal(size= Nsamps)
            y2 = .5*np.cos(8*tA) + .3*np.random.normal(size = Nsamps)
            tNow += Nsamps
            sleep(Nsamps*dt)
            print(f"generated now: {tNow}, {Nsamps} samples")
            return {'tL': list(tA.astype("i4")), 'y1': list(y1.astype("i4")), "y2":list(y2.astype('i4'))}

        sender = ZMQChunkedPublisher(port, topic="mag")
        reader = ZMQSubReadChunked(port, topic="mag")

        sendTimer = RepeatTimer(0.1, lambda: sender.send(**generate_data()) )
        sendTimer.start()
        tStart = time.time()
        while time.time()-tStart < 15:
            datD, Nread = reader.retrieve(20)
            if Nread>0:
                print(f"Nread: {Nread}, tNow: {time.time()}")
                print(datD.keys())
        sendTimer.stop()
        del sendTimer;

    def test_transformer():
        def generateTraces():
            global tNow
            Ntraces = 1 + int(10*random.uniform())
            y = list(random.normal(size= (Ntraces, 100)))
            t = list(tNow +arange(Ntraces))
            tNow += Ntraces
            print(f'sending... {len(t), len(y)}')
            return t, y
        sender = ZMQPubSendData(5999, topic ="raw")


        if 1:
            tNow = 0


            sendTimer = RepeatTimer(0.1, lambda: sender.send(*generateTraces()) )
            sendTimer.start()
            #sendTimer = RepeatTimer(0.1, lambda: generateTraces() )

            def sumAll(args):
                t, data = args
                return t, data.sum(axis=-1)
            sumServer = AsyncTransformer(
                    inputF = ZMQSubReadData(port = 5999, topic = "raw").retrieve,
                    transformF = sumAll,
                    outputF = lambda args: ZMQPubSendData(6000, "summed").send(*args),
                    run_interval = 0.2,
                    )
            sumServer.start()

    from PyQt5 import QtTest
    def test_async_plotter():
        topic = "Mag"
        port = 5001
        tNow = 0
        dt = 0.1
        def generate_dict_data(): # Generate some data to plot
            try:
                nonlocal tNow
                tElapsed = time.time() - tNow - t0
                Nsamps = int(tElapsed/dt)
                tA = tNow + dt*np.arange(Nsamps)
                y1 = 2*np.sin(5*tA) + .1*np.random.normal(size= Nsamps)
                y2 = .5*np.cos(8*tA) + .3*np.random.normal(size = Nsamps)
                tNow += tElapsed
                #Nsamps = int(5 + 5*(np.random.uniform()**2))
                sleep((5 + 5*(np.random.uniform()**2))*dt)
                print(f"generated now: {tNow}, {Nsamps} samples")
                return {'t': list(tA.astype("f4")), 'y1': list(y1.astype("f4")), "y2":list(y2.astype('f4'))}
            except Exception as e:
                print("exception in generattion")
                track = traceback.format_exc()
                print(track)


        sender = ZMQChunkedPublisher(port, topic=topic)
        sendTimer = RepeatTimer(0.1, lambda: sender.send(**generate_dict_data()) )
        t0 = time.time()
        sendTimer.start()
        reader = ZMQSubReadChunked(port = port, topic = topic)
        plotter = ChunkedSourcePlotter(
                inputF = lambda : reader.retrieve(20),
                label = topic,
                poll_interval=0.2
                )
        #plotter.start()
        #for k in range(20):
            #plotter.update()
            #time.sleep(.5)
            #QtTest.QTest.qWait(300)
        return plotter
    def test_async_plotter2():

        topic = "Mag"
        port = 5001

        tNow = 0
        dt = .2
        def generate_dict_data(): # Generate some data to plot
            nonlocal tNow
            Nsamps = int(5 + 5*(np.random.uniform()**2))
            tA = tNow + dt*np.arange(Nsamps)
            y1 = 2*np.sin(5*tA) + .1*np.random.normal(size= Nsamps)
            y2 = .5*np.cos(8*tA) + .3*np.random.normal(size = Nsamps)
            tNow += Nsamps
            sleep(Nsamps*dt)
            print(f"generated now: {tNow}, {Nsamps} samples")
            return {'tL': list(tA.astype("i4")), 'y1': list(y1.astype("i4")), "y2":list(y2.astype('i4'))}

        sender = ZMQChunkedPublisher(port, topic=topic)
        sendTimer = RepeatTimer(0.1, lambda: sender.send(**generate_dict_data()) )
        sendTimer.start()
        #sendTimer = RepeatTimer(0.1, lambda: generateTraces() )



        from DPM import DockPlotManager
        dpm = DockPlotManager(f"{topic} Plot", defaultPlotKwargs = dict(mode = 1))
        #This should really be handled by DPM itself, but isn't currently
        def addToDPM(recieved):
            print("addToDPM run")
            if recieved is not None:
                datD, N = recieved
                print(f"addToDPM recieved {N}")
                tL = datD.pop('tL')
                for key in datD:
                    dpm.addData(key, {'x':tL,'y': datD[key]})



        sumServer = AsyncTransformer(
                inputF = ZMQSubReadChunked(port = port, topic = topic).retrieve,
                transformF = lambda args: addToDPM(args),
                outputF = lambda *_: None,
                run_interval = 0.2,
                )
        sumServer.start()

    #test_ZMQ_chunked_pub()
    plotter = test_async_plotter()
