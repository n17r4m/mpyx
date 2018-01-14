"""
Retrieve a frame from a video.

Author: Martin Humphreys / Kevin Gordon
"""


from skimage import color
from skvideo.io import vread
import sys
import pims
import numpy as np



class Video:
  
    def __init__(self, video, gray=True):
        self.reader, self.file = Video.getVideoReader(video)
        self.gray = gray
        self.bg = None
        self.frames = len(self.reader)
        self.height, self.width, self.channels = self.reader[0].shape
        
        self.correct_shape=True

        self.shape = (self.height, self.width, 1 if gray else self.channels)
        self.e = sys.float_info.min
        
    
        
    def __len__(self):
        return self.frames

    def frame(self, frame_no=0):
        
        frame = self.reader[frame_no]
            
        if not len(frame):
            print("Error reading video frame " + str(frame_no))
            return None
        else:
            if self.gray:
                frame = np.expand_dims(color.rgb2gray(frame), axis=-1)
            return frame
    
    def extract_background(self, n = 100):  
        if self.bg is None:
            
            #avg_bg = np.zeros(self.shape)
            max_bg = np.zeros(self.shape)
            for i in range(0, 80):
                frame = self.frame(i)
                max_bg = np.maximum(frame, max_bg)
                #avg_bg += frame / n
            #self.bg = (max_bg + avg_bg + self.e) / 2.0
            self.bg = max_bg
            
            
        return self.bg
    
    def normal_frame(self, frame_no=0):
        div = self.frame(frame_no) / self.extract_background()
        return (255.0 * np.clip(div, 0, 1)).astype("uint8")
    

    @staticmethod
    def getVideoReader(video):
        if isinstance(video, str):
            return vread(video, verbosity=0), video
            """
            if video.split(".")[-1] == "avi":
                return pims.open(video), video
            else:
                return vread(video), video
            """
        else:
            return video.reader, video.file
 
