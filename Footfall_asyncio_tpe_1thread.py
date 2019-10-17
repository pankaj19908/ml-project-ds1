import argparse
import time
import asyncio
import concurrent.futures
import threading

from models import *
from utils.datasets import *
from utils.utils import *
import os
import sys
from datetime import datetime
import cv2
import glob
import random
import argparse
import torch
from PIL import Image
import matplotlib as mpl
mpl.use('TkAgg') 
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.ticker import NullLocator
from skimage.transform import resize
import torchvision.transforms as transforms
import numpy as np
import skimage.io
from kafka import KafkaClient
from kafka import SimpleProducer
from kafka import KafkaProducer
import threading
import MySQLdb
jjjj

jjkjkjhjhjhgjhgjh

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka, async=False)

from kafka import KafkaConsumer
group_name = "my-group"
consumer_topic_name = "footfall-pipeline-image-capture-python"
consumer = KafkaConsumer(consumer_topic_name, bootstrap_servers='localhost:9092', auto_offset_reset='latest',enable_auto_commit='true')
db = MySQLdb.connect(host="127.0.0.1",user="root",password="viena@ds123",database="viena")

cuda = torch.cuda.is_available()
device = torch.device('cuda:0' if cuda else 'cpu')

parser = argparse.ArgumentParser()
# Get data configuration

parser.add_argument('-image_folder', type=str, default='data/samples', help='path to images')
parser.add_argument('-output_folder', type=str, default='output', help='path to outputs')
parser.add_argument('-plot_flag', type=bool, default=True)
parser.add_argument('-txt_out', type=bool, default=True)

parser.add_argument('-cfg', type=str, default='cfg/package.cfg', help='cfg file path')
parser.add_argument('-class_path', type=str, default='data/package.names', help='path to class label file')
parser.add_argument('-conf_thres', type=float, default=0.50, help='object confidence threshold')
parser.add_argument('-nms_thres', type=float, default=0.45, help='iou threshold for non-maximum suppression')
parser.add_argument('-batch_size', type=int, default=1, help='size of the batches')
parser.add_argument('-img_size', type=int, default=32 * 13, help='size of each image dimension')
opt = parser.parse_args()
print(opt)

os.system('rm -rf ' + opt.output_folder)
os.makedirs(opt.output_folder, exist_ok=True)

# Load model
model = Darknet(opt.cfg, opt.img_size)

weights_path = 'checkpoints/package_80000_with_video1_video2_FPB_5aug.weights'

if weights_path.endswith('.weights'):  # saved in darknet format
    load_weights(model, weights_path)
else:  # endswith('.pt'), saved in pytorch format
    checkpoint = torch.load(weights_path, map_location='cpu')
    model.load_state_dict(checkpoint['model'])
    del checkpoint

model.to(device).eval()

# Set Dataloader
#classes = load_classes(opt.class_path)  # Extracts class labels from file
#dataloader = load_images(opt.image_folder, batch_size=opt.batch_size, img_size=opt.img_size)
#input_path ='data/samples/11.jpg'
img_size=32 * 13
classes = load_classes(opt.class_path)  # Extracts class labels from file
dataloader = load_images(opt.image_folder, batch_size=opt.batch_size, img_size=opt.img_size)
imgs = []  # Stores image paths
img_detections = []  # Stores detections for each image index
prev_time = time.time()



async def main(executor):
    cnt = 0
    try:
        loop1 = asyncio.new_event_loop()

        for msg in consumer:
            print("Footfall - process is called---")
            msg2 = msg.value
            msg3 = msg2.decode()

            value = msg3.split("||")

            input_path = value[0]
            image_id = value[1]
            created_time = value[2]
            videoId =  value[3]
            no_of_fps_to_read = value[4]
            status = value[5]

            if(status == "completed"):
                print("1. processCompletedVideo: "+videoId+"|"+created_time)

            else:
                try:
                    print("2. process in progress : "+videoId+"|"+created_time)
                except Exception as e:
                    print(e)
            cnt = cnt+1
    except Exception as e:
        print(e)
		
send_to_topic = "footfall-image-yolo-completed-207"

if __name__ == '__main__':
    # Create a limited thread pool.
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1,
    )

    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(executor))
    finally:
        event_loop.close()
