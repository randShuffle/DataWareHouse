import numpy as np
import pandas as pd
import asyncio
import httpx
from lxml import etree
from selenium import webdriver
import random
from fake_useragent import FakeUserAgent
import requests
import aiofiles
import os
from selenium.webdriver.chrome.options import Options
columns=["ASIN","Release date","Genre","Director","Actors","Producers","Starring","Run time","Language","Version"]


base_url="https://www.amazon.com/dp/"

productIds=np.load("data/productId.npy",allow_pickle=True)[:253000]

ua=FakeUserAgent()
 



class MyReptile:
    columns=["ASIN","Release date","Genre","Director","Actors","Producers","Starring","Run time","Language","Version"]
   
    MAX_EXCEPTION_TIMES=5
    TIME_OUT=10

    #proxy format is as below
    #proxy_list = {'http://': 'http://8.134.138.108:8888',}



    def clear_all(self):
        self.data.clear()
        self.fault_asin.clear()
        self.neglect_asin.clear()
        self.proxy_list.clear()
        self.cookie_pool.clear()
        self.parse_fault_asin.clear()

    def __init__(self,t_epoch) -> None:
        self.iii=0
        self.data=[]
        self.fault_asin=[]
        self.neglect_asin=[]
        self.parse_fault_asin=[]
        self.proxy_list=[]
        self.cookie_pool=[]
        self.generate_proxy_pool()
        self.CONCURRENCY_NUMBER=1
        self.SEMAPHORE=asyncio.Semaphore(self.CONCURRENCY_NUMBER)
        self.epoch=t_epoch
        self.not_found_asin=[]

    def generate_proxy_pool(self):
       
        while True:
            try:
                print("new")
                self.proxy_list.clear()
                response=requests.get("http://127.0.0.1:5010/all/").json()
                self.proxy_list=[proxy['proxy'] for proxy in response]
                self.CONCURRENCY_NUMBER=min(1,len(self.proxy_list))
                break
            except:
                pass
        pass



    def parseWhite(self,response,asin):
        tree = etree.HTML(response)
        keys=tree.xpath('//*[@id="detailBullets_feature_div"]/ul/li[*]/span/span[1]/text()')
        keys=[k.split("\n",1)[0] for k in keys]    
        values=tree.xpath('//*[@id="detailBullets_feature_div"]/ul/li[*]/span/span[2]/text()')
        values=[v.split("\n",1)[0] for v in values] 
        
        
        #get Genre info
        key_list=tree.xpath('//*[@id="productOverview_feature_div"]/div/table/tr[*]/td[1]/span/text()')
        value_list=tree.xpath('//*[@id="productOverview_feature_div"]/div/table/tr[*]/td[2]/span/text()')
        
        key_value_zip=zip(key_list,value_list)
        for item in key_value_zip:
            if "Genre"==item[0] or "Language"==item[0]:
                keys.append(item[0])
                values.append(item[1])
                
        #get version info
        version_list=tree.xpath('//*[@id="tmmSwatches"]/ul/li[*]/span/span/span/a/span[1]/text()')
        if len(version_list):
            keys.append("Version")
            values.append(','.join(version_list))

        #add asin info
        keys.append("ASIN")
        values.append(asin)

        keys_values_pair=dict(zip(keys,values))

        data_single=[]
        for key in self.columns:
            if key in keys_values_pair.keys():
                data_single.append(keys_values_pair[key])
            else:
                data_single.append(np.nan)

        self.data.append(data_single)

    def parseBlack(self,response,asin):
        tree = etree.HTML(response)
        keys=tree.xpath('//*[@id="btf-product-details"]/div[1]/dl[*]/dt/span/text()') 
        values=[]
        #多值属性：//*[@id="btf-product-details"]/div[1]/dl[x]/dd/a 
        #单值属性：//*[@id="btf-product-details"]/div[1]/dl[x]/dd
        for i,key in enumerate(keys):
            if key=="Directors":
                keys[i]='Director'
            elif key=='Audio languages':
                keys[i]="Language"
            mul_val_pattern=f'//*[@id="btf-product-details"]/div[1]/dl[{i+1}]/dd/a/text()'
            single_val_pattern=f'//*[@id="btf-product-details"]/div[1]/dl[{i+1}]/dd/text()'
            single_val_get=tree.xpath(single_val_pattern) 
            if not len(single_val_get):
                values.append(','.join(tree.xpath(mul_val_pattern)))
            else:
                values.append(single_val_get[0])

        #add asin info
        keys.append("ASIN")
        values.append(asin)
        #runtime
        runtime_val=tree.xpath('//*[@id="main"]/div[1]/div/div/div[2]/div[3]/div/div[2]/div[3]/div/span[@data-automation-id="runtime-badge"]/text()')
        if len(runtime_val):
            keys.append("Run time")
            values.append(','.join(runtime_val))
        #release time
        release_val=tree.xpath('//*[@id="main"]/div[1]/div/div/div[2]/div[3]/div/div[2]/div[3]/div/span[@data-automation-id="release-year-badge"]/text()')
        if len(release_val):
            keys.append("Release date")
            values.append(','.join(release_val))
        #genre
        genre_val=tree.xpath('//*[@id="main"]/div[1]/div/div/div[2]/div[3]/div/div[2]/div[4]/div/span[*]/a/text()')
        if len(genre_val):
            keys.append("Genre")
            values.append(','.join(genre_val))

        #version/format
        version_val=tree.xpath('//*[@id="btf-product-details"]/div[2]/div/div/a/span/strong/text()')
        if len(version_val):
            keys.append("Version")
            values.append(','.join(version_val))

        keys_values_pair=dict(zip(keys,values))
        data_single=[]
        for key in self.columns:
            if key in keys_values_pair.keys():
                data_single.append(keys_values_pair[key])
            else:
                data_single.append(np.nan)
        self.data.append(data_single)

    def whiteOrBlack(self,response)->int:
        tree = etree.HTML(response)
        
        if not len(tree.xpath('//*[@id="nav-search-label-id"]/text()')):
            return 3       
        type=tree.xpath('//*[@id="nav-search-label-id"]/text()')[0]
        if type=='Movies & TV':
            return 0
        elif type=="Prime Video":
            return 1
        else:
            return 2
        
    def parse(self,response,asin):
        judge=self.whiteOrBlack(response)  
        try:  
            if judge==0:
                self.parseWhite(response,asin)
            elif judge==1:
                self.parseBlack(response,asin)
            else:
                self.neglect_asin.append(asin)
        except:
            self.parse_fault_asin.append(asin)

    def get_proxy(self):
        if not len(self.proxy_list):
            self.generate_proxy_pool()
        proxy=random.choice(self.proxy_list)

        return {'http://': "http://"+proxy}

    def remove_proxy(self,proxy):
        try:
            self.proxy_list.remove(proxy.split("//")[-1])      
            #print("remove:"+proxy)
        except:
            pass
        finally:
            if len(self.proxy_list)<1:
                    self.generate_proxy_pool()
            

    #pool中cookie数量大于10才可随机选择
    def get_cookie(self):
        if len(self.cookie_pool)>=10:
            return random.choice(self.cookie_pool)
        else:
            return None

    def remove_cookie(self,cookie):
        try:
            self.cookie_pool.remove(cookie)
            #print("remove cookie")
        except:
            pass
            #print("remove cookie error")
        

    def add_cookie(self,cookie):
        if cookie not in self.cookie_pool:
            self.cookie_pool.append(cookie)


   

    async def get_with_httpx(self,url,asin):
        
        headers = {
          'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36',
          'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6',
          'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
          'accept-encoding': 'gzip, deflate, br',
          "Referer": "https://www.amazon.com/",
        }
        headers["User-Agent"]=ua.random
        exception_times=0
        while exception_times<self.MAX_EXCEPTION_TIMES:
            try:
                async with self.SEMAPHORE:
                    proxy=self.get_proxy()
                    async with httpx.AsyncClient(proxies=proxy,timeout=self.TIME_OUT) as client:
                        rand_cookie=self.get_cookie()
                        #只给2次机会，第一次要是不行，第二次直接不使用原来的cookie
                        for i in range(2):
                            try:
                                response=await client.get(url=url,headers=headers,cookies=rand_cookie)
                                if response.status_code=='404':
                                    self.not_found_asin.append(asin)
                                response.encoding='utf-8'
                                #这里就是处罚验证码bug了
                                if "DOCTYPE" in response.text[0:20]:
                                    print("captcha found.remove proxy")
                                    i=3
                                    raise ""

                                if i==0 and rand_cookie==None:
                                    self.add_cookie(response.text)

                                break                               
                            except:
                                if i==1 or i==3:
                                    raise ""
                                else:
                                    self.remove_cookie(rand_cookie)
                                    rand_cookie=None

                        #self.parse(response.text,asin)
                        async with aiofiles.open(f"C:\\amazon\\epoch{self.epoch}\\{asin}.html", 'w',encoding='utf-8') as file:
                            await file.write(response.text)

                        
                        self.iii+=1
                        print(self.iii)
                        #只要走过parse，所有的错误都是解析异常，和请求没有关系！
                        #随后把它写入文件
                        return
            except:
                self.remove_proxy(proxy["http://"])
                exception_times+=1
                if exception_times==self.MAX_EXCEPTION_TIMES:
                    self.fault_asin.append(asin)
                    #print("fault")
                print(asin+"exception times"+str(exception_times))
                
        
        

   
           

    async def pipeline(self,asins):
        get_movie_tasks=[self.get_with_httpx(base_url+productId,productId)for productId in asins]
        await asyncio.wait(get_movie_tasks)



productIds=productIds.reshape(-1,50)

loop_times=productIds.shape[0]





for i in range(loop_times):
    os.mkdir(f"C:\\amazon\\epoch{i+100}")
    r=MyReptile(i+100)
    # for x in productIds:
    #     r.get_with_selenium(base_url+x,x)
    asyncio.run(r.pipeline(productIds[i]))
    #df=pd.DataFrame(data=r.data,columns=r.columns)
    df_fault=pd.DataFrame(data=r.fault_asin,columns=['ASIN'])
    df_notfound=pd.DataFrame(data=r.not_found_asin,columns=['ASIN'])
    #df_neglect=pd.DataFrame(data=r.neglect_asin,columns=['ASIN'])
    #df_parse_fault=pd.DataFrame(data=r.parse_fault_asin,columns=['ASIN'])
    #df.to_csv(f'./ans/{i+50}.csv',index=False)
    df_fault.to_csv(f"./fault/fault{i+100}.csv",index=False)
    df_notfound.to_csv(f"./not_found/not_found{i+100}.csv",index=False)
    #df_neglect.to_csv(f"./ans/{i+50}neglect.csv",index=False) 
    #df_parse_fault.to_csv(f"./ans/{i+50}parsefault.csv",index=False) 

    print("epoch"+str(i+100))
    #print(r.data)
    print('----------------------')
    print(len(r.proxy_list))
    print('----------------------')
    
    break
















