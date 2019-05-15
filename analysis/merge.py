import os
import codecs
import traceback
import pandas as pd
from pandas import Series

root_path = '/home/xwy/project/lianjia-beike-spider/data/ke/ershou/bj'

regions = ['dongcheng', 'xicheng', 'haidian', 'chaoyang', 'shijingshan', 'fengtai', 
           'tongzhou', 'fangshan', 'daxing', 'shunyi', 'pinggu', 'changping', 'huairou',
           'miyun', 'mentougou', 'yizhuangkaifaqu', 'yanqing']

col_header = ['info', 'year', 'layout', 'size', 'description']

data_paths = []
for dirpath, dirnames, filenames in os.walk(root_path):
    for subdir in dirnames:
        path = os.path.join(dirpath, subdir)
        data_paths.append(path)
        
        
def merge(data_path):
    """
    merge files by region
    """
    region_file_dict = {}
    
    # remove old files
    for region in regions:
        removed_file = os.path.join(data_path, region + ".csv")
        print removed_file
        if os.path.exists(removed_file):
            os.remove(removed_file)
    
    # create region dictionary
    for _, _, filenames in os.walk(data_path):
        for f in filenames:
            region = f.split('_')[0]
            if not region in region_file_dict:
                region_file_dict[region] = []
            region_file_dict[region].append(os.path.join(data_path, f))
    
    for region, files in region_file_dict.items():
        new_fname = os.path.join(data_path, region + ".csv")
        with codecs.open(new_fname, "a+", encoding="utf-8") as new_f:
#             print new_fname
            for fname in files:
#                 print fname
                try:
                    for txt in codecs.open(fname, "r", encoding="utf-8"):
                        new_f.write(txt)
                except Exception as e:
                    traceback.format_exc(e)
        
        
def read_file(data_path):
    data_dict = {}
    for region in regions:
        print os.path.join(data_path, region + ".csv")
        region_data = pd.read_csv(os.path.join(data_path, region + ".csv"), sep="|", encoding='utf-8', names=col_header, header=None)
        region_data = extract_info(region_data)
        data_dict[region] = region_data
        
    return data_dict
    
def extract_info(data):
    named_cols = {0: "date", 1: "region", 2: "area", 4: "price", 5: "floor"}
    info_df = data['info'].str.split(',', expand=True)
    info_df.rename(columns=named_cols, inplace=True)
#     info_df[named_cols.values()]
    data = data.join(info_df[named_cols.values()])
    return data
    
    
for data_path in data_paths:
#     merge(data_path)
    data_dict = read_file(os.path.join(root_path, "20190426"))
    break

data_dict['changping']
