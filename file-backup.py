 # encoding=utf-8

import shutil
import os,sys

def getFileList(dir,wildcard,recursion):
  os.chdir(dir)
  exts = wildcard.split(" ")
  files = os.listdir(dir)
  for name in files:
    fullname=os.path.join(dir,name)
    if(os.path.isdir(fullname) & recursion):
      getFileList(fullname,wildcard,recursion)
    else:
      for ext in exts:
        if(name.endswith(ext)):
          fileList.append(fullname)
          filedir.append(dir)
          filenames.append(name.lower())
  return fileList,filedir,filenames


#转码函数
def changeCode(name):
  name = name.decode('GBK')
  name = name.encode('UTF-8')
  return name  

  
if __name__ == '__main__':
  
  dirpairs={'/Users/zhaoqing/Documents/books/':'/Volumes/D/books/',
  					'/Volumes/备份盘/相机照片/':'/Volumes/D/bak/phone-photo/mac-pic20170611bak/',
  					'/Volumes/备份盘/iphone20161113/':'/Volumes/D/bak/phone-photo/mac-pic20170611bak/',
  					'/Volumes/备份盘/mac-pic20170611bak/':'/Volumes/D/bak/phone-photo/mac-pic20170611bak/'
            }
  wildcard = " "  
  
  for key in dirpairs:
    pathAdd = key
    pathTot = dirpairs[key]
    print(pathAdd)
    print(pathTot)
    
    fileList = []
    filedir = []
    filenames = []
    listAdd = getFileList(pathAdd,wildcard,1)
    
    fileList = []
    filedir = []
    filenames = []
    listTot = getFileList(pathTot,wildcard,1)
    '''
    for i in range(0,8):
    	print('listAdd:  '+ listAdd[0][i]+'\n')

    for i in range(0,8):	
    	print('listTot:  '+ listTot[0][i]+'\n')
    '''
    nfiles = len(listAdd[0])
    excludes=['screen','副本']
    
    #print(all([x not in listAdd[2][1] for x in excludes]))
    copyrecord=[]
    f=open('/Users/zhaoqing/Documents/logs.txt','w')
    for count in range(0,nfiles):
      if (listAdd[2][count] not in listTot[2]) \
      and all([ex not in listAdd[2][count] for ex in excludes]) \
      and listAdd[2][count] not in copyrecord:
        f.write(listAdd[0][count])
        f.write('\n')
        print(listAdd[0][count])
      	shutil.copyfile(listAdd[0][count],dirpairs[key]+listAdd[2][count])
      	print('copy '  +str(len(copyrecord)) + ' done!'+'\n')
      	copyrecord.append(listAdd[2][count])
    f.close
    









  
      