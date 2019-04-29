import json
import glob
import os
from concurrent import futures
import numpy as np
import os.path as osp
videos = set()


d_sets = ['/vol/bitbucket2/yl1915/youtube-bb/yt_bb_detection_train', '/vol/bitbucket2/yl1915/youtube-bb/yt_bb_detection_validation']
cls = os.listdir(d_sets[0])
print(cls)
print(len(cls))

db = []

def process_one_cls(d_set, cls):
    img_dir = osp.join(d_set,cls)
    videos_in_cls = {}
    img_list=sorted(os.listdir(osp.join(d_set,cls)))
    for img in img_list:
        vid = img[:11]
        videos_in_cls.setdefault(vid,{})
        splits = img[12:-4].split('_')
        cc,obj,ts,x1,y1,x2,y2 = splits
        fn = osp.join(img_dir, img)
        box = [int(x1),int(x2), int(y1), int(y2)]
        videos_in_cls[vid].setdefault(obj,[]).append([fn,box])
    return img_dir, [list(v.values()) for v in videos_in_cls.values()]
    

# _, test = process_one_cls(d_sets[0], '1')
# for t in test:
    # if len(t)>2:
        # v=t
        # break

# import ipdb; ipdb.set_trace()

with futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    
    ddd = {executor.submit(process_one_cls, d_sets[0],c): d_sets[0] + c for c in cls} 
    ddd.update({executor.submit(process_one_cls, d_sets[1],c): d_sets[1] + c for c in cls})
    for future in futures.as_completed(ddd):
        ccc = ddd[future]
        try:
            img_dir, vid_list = future.result()
            db+=vid_list
        except Exception as exc:
            print('Erros in processing {}'.format(ccc))
        else:
            print('\t {} finished'.format(img_dir))



with open('youtube_final.txt', 'w') as f:
    json.dump(db,f,indent=4,sort_keys=True)
