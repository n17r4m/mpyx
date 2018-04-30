from mpyx import F

import numpy as np
import subprocess as sp
import fcntl
import shlex
import time
import os

from skvideo.io import FFmpegReader, FFmpegWriter
from functools import reduce

def arg_exists(arg, args):
    return any([a.find(arg) + 1 for a in args])


class FFmpeg(F):
    # https://github.com/leandromoreira/ffmpeg-libav-tutorial#learn-ffmpeg-libav-the-hard-way
    # Do no use this. make a wrapper around skvideo instead. 
    # or, calculate correct framesize in bytes to perform correctly aligned reads,
    # 
    def initialize(self, input_url = '-', input_opts = [], output_url = '-', output_opts = [],  global_opts = [], verbose=False):
        if isinstance(input_opts, str): input_opts = [input_opts]
        if isinstance(output_opts, str): output_opts = [output_opts]
        if isinstance(global_opts, str): global_opts = [global_opts]
        
        
        
        if isinstance(input_url, tuple):
            if len(input_url) != 3:
                raise ValueError("Input tuple shape must be (H,W,C)")
                
            self.input_shape = input_url
            self.input_frame_size = reduce(lambda n, x: n*x, self.input_shape)
            input_url = "-"
            
            if not arg_exists("-f", input_opts):
                input_opts.append("-f rawvideo")
            
            if not arg_exists("video_size", input_opts):
                input_opts.append("-video_size {}x{}".format(self.input_shape[1], self.input_shape[0]))
            
            if not arg_exists("pix_fmt", input_opts):
                if self.input_shape[2] == 1:
                    input_opts.append("-pix_fmt gray")
                if self.input_shape[2] == 3:
                    input_opts.append("-pix_fmt rgb24")
            
        
        
        
                
        if isinstance(output_url, tuple):
            if len(output_url) != 3:
                raise ValueError("Output tuple shape must be (H,W,C)")
                
            self.output_shape = output_url
            self.output_frame_size = reduce(lambda n, x: n*x, self.output_shape)
            output_url = "-"
            
            if not arg_exists("-f", output_opts):
                output_opts.append("-f rawvideo")
            
            if not arg_exists("video_size", output_opts):
                output_opts.append("-video_size {}x{}".format(*self.output_shape))
            
            if not arg_exists("pix_fmt", output_opts):
                if self.output_shape[2] == 1:
                    output_opts.append("-pix_fmt gray")
                if self.output_shape[2] == 3:
                    output_opts.append("-pix_fmt rgb24")
        
        
        self.command = ' '.join(
            ['ffmpeg'] 
            + global_opts 
            + input_opts
            + ['-i', input_url] 
            + output_opts 
            + [output_url]
        )
        self.is_input_stream = input_url == '-'
        self.is_output_stream = output_url == '-'
        self.verbose = verbose
        
    
    def setup(self, *init_args):
        if self.verbose:
            print(self.command)
            
        self.proc = sp.Popen(shlex.split(self.command), stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
        #fcntl.fcntl(self.proc.stdin.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
        #fcntl.fcntl(self.proc.stdout.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(self.proc.stderr.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
        
        if not self.is_input_stream and self.is_output_stream:
            # emit video frames from file
            
            
            frame_buf = None
            needed_bytes = self.output_frame_size
            
            while self.proc.poll() is None:
                frame_bytes = self.proc.stdout.read(self.output_frame_size)
                
                if len(frame_bytes) == self.output_frame_size:
                
                    frame = np.fromstring(frame_bytes, dtype='uint8').reshape(self.output_shape)
                    self.put(frame)
                
                err = self.proc.stderr.read()
                if self.verbose and err != "" and err is not None:
                    print("FFmpeg:", str(err))

            self.stop()
    
    def do(self, frame):
        try:
            if self.is_input_stream:
                self.proc.stdin.write(frame.tobytes())
        except Exception as e:
            print("ERRRRR, could not write to FFmpeg stream", type(e), e)
        
        
        try:
            if self.is_output_stream:
                frame_bytes = self.proc.stdout.read(self.output_frame_size)
                if len(frame_bytes) == self.output_frame_size:
                    frame = np.fromstring(frame_bytes, dtype='uint8').reshape(self.output_shape)
                    self.put(frame)
        except Exception as e:
            print("ERRRRR, could not read from FFmpeg stream", type(e), e)
            
            
        try:
            err = self.proc.stderr.read()
            if self.verbose and err != "" and err is not None:
                print("FFmpeg:", str(err))
        except:
            pass
        
    
    def teardown(self):
        print("FFmpeg Teardown")
        while self.proc.poll() is None:
            
            if self.is_input_stream:
                self.proc.stdin.close()
                
            
            if self.is_output_stream:
                try:
                    frame_bytes = self.proc.stdout.read(self.output_frame_size)
                    if len(frame_bytes) == self.output_frame_size:
                        frame = np.fromstring(frame_bytes, dtype='uint8').reshape(self.output_shape)
                        self.put(frame)
                except Exception as e:
                    print("ERRRRR, could not read from FFmpeg stream", type(e), e)
                
                try:
                    err = self.proc.stderr.read()
                    if self.verbose and err != "" and err is not None:
                        print("FFmpeg:", str(err))
                except:
                    pass
            
            else:
                time.sleep(0.1)
    
    

from collections import deque
import math

class BG(F):
    
    def setup(self, model = "median", window_size = 20, *args, env=None, **kwArgs):

        self.que = deque(maxlen=math.ceil(window_size / 2))
        self.model = getattr(self, model)(window_size=window_size, *args, **kwArgs)
        
    def do(self, frame):
        
        self.que.append((self.meta, frame))
        self.bg = self.model.process(frame)
        
        
        if isinstance(self.model, (self.mog, self.knn)):
            self.put({"frame": frame, "bg": self.bg})
            
        else:
            if len(self.que) == self.que.maxlen:
                self.meta, frame = self.que.popleft()
                self.put({"frame": frame, "bg": self.bg})

    def teardown(self):
        while len(self.que) > 0:
            self.meta, frame = self.que.popleft()
            self.put({"frame": frame, "bg": self.bg})


    class median:
        
        def __init__(self, window_size = 20, img_shape = None):
            self.que = deque(maxlen=window_size)
        
        def process(self, frame):
            self.que.append(frame)
            return np.median(self.que, axis=0).astype('uint8')
    
    class splitmedian:
        
        def __init__(self, window_size = 20, img_shape = None):
            self.que = deque(maxlen=window_size)
        
        def process(self, frame):
            self.que.append(frame)
            ends = list(self.que)[:math.ceil(len(self.que)/3)] + list(self.que)[math.floor(2*len(self.que)/3):]
            return np.median(ends, axis=0).astype('uint8')
    
    class mean:
        
        def __init__(self, window_size = 20, img_shape = None):
            self.que = deque(maxlen=window_size)
        
        def process(self, frame):
            self.que.append(frame)
            return np.mean(self.que, axis=0).astype('uint8')
            
    class splitmean:
        
        def __init__(self, window_size = 20, img_shape = None):
            self.que = deque(maxlen=window_size)
        
        def process(self, frame):
            self.que.append(frame)
            ends = list(self.que)[:math.ceil(len(self.que)/3)] + list(self.que)[math.floor(2*len(self.que)/3):]
            return np.mean(ends, axis=0).astype('uint8')
    
    class max:
        
        def __init__(self, window_size = 20, img_shape = None):
            self.que = deque(maxlen=window_size)
        
        def process(self, frame):
            self.que.append(frame)
            return np.max(self.que, axis=0).astype('uint8')
    
    class splitmax:
        
        def __init__(self, window_size = 20, img_shape = None):
            self.que = deque(maxlen=window_size)
        
        def process(self, frame):
            self.que.append(frame)
            ends = list(self.que)[:math.ceil(len(self.que)/3)] + list(self.que)[math.floor(2*len(self.que)/3):]
            return np.max(ends, axis=0).astype('uint8')
    
    class ae:
        
        def __init__(self, window_size = 20, img_shape = (1729,2336,3)):
            import tensorflow as tf
            
            Input, Dense = tf.keras.layers.Input, tf.keras.layers.Dense
            Model = tf.keras.models.Model
            
            self.img_shape = img_shape
            
            self.que = deque(maxlen=window_size)
            
            img_size = img_shape[0] * img_shape[1] * img_shape[2]
            
            input_img = Input(shape=(img_size,))
            
            initr = tf.keras.initializers.RandomUniform(minval=0.0000001, maxval=0.0002)
            
            encoded = Dense(4, activation='selu', kernel_initializer=initr)(input_img)
            decoded = Dense(img_size, activation='selu', kernel_initializer=initr)(encoded)
            
            autoencoder = Model(input_img, decoded)
            
            opti = tf.keras.optimizers.Adadelta(lr=0.2, rho=0.95, decay=0.0)
            #opti = tf.keras.optimizers.SGD(lr=10, momentum=0.25, decay=0.0)
            
            #autoencoder.compile(optimizer='adadelta', loss='binary_crossentropy')
            autoencoder.compile(optimizer=opti, loss='mse')
            
            self.autoencoder = autoencoder
        
        def process(self, frame):
            
            frame = frame.flatten().squeeze() / 255.0
            
            self.que.append(frame)
            
            batch = np.array(self.que)
            
            self.autoencoder.fit(batch, batch, epochs=1, 
                batch_size=len(self.que),
                #batch_size=1,
                shuffle=True)
            
            
            
            bg = (self.autoencoder.predict(np.array([frame]))[0] * 255)
            
            return bg.reshape(self.img_shape).astype('uint8')
    
    class mog:
        
        def __init__(self, window_size = None, img_shape = None):
            
            import cv2
            self.fgbg = cv2.createBackgroundSubtractorMOG2(detectShadows=False)
            self.kern = cv2.getStructuringElement(cv2.MORPH_ELLIPSE,(3,3))
        
        def process(self, frame):
            
            import cv2
            
            fgmask = self.fgbg.apply(frame)
            
            fgmask[fgmask < 0.75] = 0
            
            fgmask = cv2.morphologyEx(fgmask, cv2.MORPH_OPEN, self.kern)
            
            cv2.imshow("mask", fgmask)
            cv2.waitKey(1)
            
            return frame * (1 - fgmask[:, :, np.newaxis])
    
    class knn:
        
        def __init__(self, window_size = None, img_shape = None):
            
            import cv2
            self.fgbg = cv2.createBackgroundSubtractorKNN(detectShadows=True)
            self.kern = cv2.getStructuringElement(cv2.MORPH_ELLIPSE,(3,3))
            
        def process(self, frame):
            import cv2
            fgmask = self.fgbg.apply(frame)
            
            fgmask[fgmask < 0.75] = 0
            fgmask = cv2.morphologyEx(fgmask, cv2.MORPH_OPEN, self.kern)
            
            cv2.imshow("mask", fgmask)
            cv2.waitKey(1)
            
            return frame * (1 - fgmask[:, :, np.newaxis])
    
    class b2d:
        
        def __init__(self, window_size=15, img_shape=None):
            
            from astropy.stats import SigmaClip
            from photutils import Background2D, MedianBackground
            
            self.sigma_clip = SigmaClip(sigma=3., iters=10)
            self.bkg_estimator = MedianBackground()
            self.B2D = Background2D
        
        def process(self, frame):
            import cv2
            
            bkg = self.B2D(frame, (50, 50), filter_size=(3, 3),
                    sigma_clip=self.sigma_clip, bkg_estimator=self.bkg_estimator)
            
            
            cv2.imshow("bkg", bkg.background)
            cv2.waitKey(1)
            
            self.que.append(bkg.background)
            
            return np.mean(self.que)
            
    class simpleMax:
        def __init__(self, window_size = 20, img_shape = None):
            self.window_size = window_size
            self.que = deque(maxlen=window_size)
            self.bg = None
        
        def process(self, frame):
            self.que.append(frame)
            if self.bg is None:
                self.bg = np.max(self.que, axis=0).astype('uint8')
            return self.bg
            
class FG(F):
    def setup(self, model = "division", *args, **kwargs):
        # If your process needs to do any kind of setup once it has been forked,
        # or if it the first process in a workflow and expected to generate 
        # values for the rest of the pipeline, that code should go here.
        self.model = getattr(self, model)()
        
    def do(self, item):
        # The main workhorse of a process. Items will flow in here, potentially
        # be modified, mapped, reduced, or otherwise morgified, and output can 
        # be then pushed downstream using the self.put() method. 
        # Here, for example, any items are simply passed along.
        fg = self.model.process(item)
        d = {'fg':fg, "frame": item["frame"]}
        self.put(d)
        
    class division:
        def __init__(self):
            pass
        
        def process(self, item):
            '''
            Expects a dict with bg, frame
            '''
            div = item["frame"] / item["bg"]
            return (255.0 * np.clip(div, 0, 1)).astype("uint8")


from skimage.filters import threshold_sauvola
from skimage.morphology import binary_opening, remove_small_objects, square
from skimage.segmentation import clear_border

class Binary(F):
    def setup(self, model = "simple", *args, **kwargs):
        # If your process needs to do any kind of setup once it has been forked,
        # or if it the first process in a workflow and expected to generate 
        # values for the rest of the pipeline, that code should go here.
        self.model = getattr(self, model)(*args, **kwargs)
        
    def do(self, item):
        # The main workhorse of a process. Items will flow in here, potentially
        # be modified, mapped, reduced, or otherwise morgified, and output can 
        # be then pushed downstream using the self.put() method. 
        # Here, for example, any items are simply passed along.
        self.put(self.model.process(item))
        
    class sauvola:
        def __init__(self):
            pass
        
        def process(self, frame):
            maxVal = 1.0
            threshold = threshold_sauvola(frame)
            return (frame > threshold) * maxVal
    
    class simple:
        def __init__(self, threshold=120):
            self.threshold = threshold
            pass
        
        def process(self, frame):
            maxVal = 1.0
            return (frame > self.threshold) * maxVal
        
    class legacyLabeled:
        def __init__(self, threshold=120):
            self.threshold = threshold
            print("Notice: legacyLabeled removes 200 pixel border!")
            pass
        
        def process(self, frame):
            maxVal = 1.0
            # Take the center, removing edge artifacts
            frame = frame[200:-200,200:-200]
            sframe = frame.squeeze()
            binary = sframe < self.threshold
            opened = binary_opening(binary, square(3))
            cleared = clear_border(opened)
            return cleared * maxVal