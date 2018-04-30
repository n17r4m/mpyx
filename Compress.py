from mpyx.F import F

import subprocess as sp
import fcntl
import shlex
import time
import os

class VideoStream(F):
    
    def initialize(self, experiment_dir, fname, width=2336, height=1729, fps=300., rate=24., pix_format="gray"):
        print("Compress.py Notice: kevin changed do() to check for tuples... temporary?")
        self.cmd = ''.join(('ffmpeg',
          ' -f rawvideo -pix_fmt {}'.format(pix_format),
          ' -video_size {}x{}'.format(width,height),
          ' -framerate {}'.format(fps),
          ' -i -',
          ' -c:v libx264 -crf 15 -preset fast',
          ' -pix_fmt yuv420p',
          ' -filter:v "setpts={}*PTS'.format(fps/rate),
          ', crop={}:{}:0:0"'.format(width, height-1) if height % 2 else '"',
          ' -r 24',
          ' -movflags +faststart',
          ' "{}"'.format(os.path.join(experiment_dir, fname))))
          
        self.proc = sp.Popen(shlex.split(self.cmd), stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)
        fcntl.fcntl(self.proc.stdout.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
        
    
    def do(self, frame_bytes):
        try:
            self.proc.stdin.write(frame_bytes)
        except TypeError:
            self.proc.stdin.write(frame_bytes[1][1])
        try:
            self.put(self.proc.stdout.read())
        except:
            pass
        

    def teardown(self):
        self.proc.stdin.close()
        self.proc.wait()
        try:
            self.put(self.proc.stdout.read())
        except:
            pass
    

class VideoFile(F):
    
    def setup(self, video_fpath, experiment_dir, fname, width=2336, height=1729, fps=300., rate=24.):
        
        cmd = ''.join(('ffmpeg -i "{}"'.format(video_fpath),
          ' -c:v libx264 -crf 15 -preset fast',
          ' -pix_fmt yuv420p',
          ' -filter:v "setpts={}*PTS'.format(fps/rate),
          ', crop={}:{}:0:0"'.format(width, height-1) if height % 2 else '"',
          ' -r 24',
          ' -movflags +faststart',
          ' "{}"'.format(os.path.join(experiment_dir, fname))))
              
        proc = sp.Popen(shlex.split(cmd), stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
        
        while proc.poll() is None:
            try:
                self.put(proc.stdout.read())
            except:
                time.sleep(1)