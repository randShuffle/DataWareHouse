# import pandas as pd
# import numpy as np
# import re


# def run_time_to_seconds(run_time:str):
#     if run_time==None:
#         return None
#     hour,min,sec=0,0,0
#     run_time=run_time.split(' ')
#     for i,item in enumerate(run_time):
#         if 'h' in item or 'hour' in item or 'hours' in item:
#             hour=int(run_time[i-1])
#         elif 'min' in item or 'minute' in item or 'minutes' in item or 'm' in item:
#             min=int(run_time[i-1])
#         elif 's' in item or 'sec' in item or 'second' in item or 'seconds' in item:
#             sec=int(run_time[i-1])
#     return hour*3600+min*60+sec


# #根据时长 30min-4h  根据genre关键词
# def is_movie(series:pd.Series):
#     #筛选电影
#     genre_keyword = ['Music Video', 'Concert', 'Special Interest', 'Exercise', 'Fitness', 'CD', 'documentary', 'series', 'BBC', 'episode', 'season']
#     if not (series['run_time']>=30*60 and series['run_time']<=4*60*60):
#         return False
       
#     for keyword in genre_keyword:
#         if keyword in series['genre']:
#             return False 
#     return True


# def filter_movie(df:pd.DataFrame):
#     pass

# #处理一下title中【】字段以及一些多余的符号
# #通过观察数据，我发现电影标题中存在描述产品信息的`()[]`，
# #比如`Sideshow:Alive on  the Inside [VHS]`或`A Passage To India  (2-Disc Collectors Edition)`
# #它们是冗余的电影标题信息，利用正则表达式可以很容易地将字符串中的括号匹配出来并删除；
# #此外，电影标题中还存在引号不规范、括号不匹配、换行符多余的情况，在这里都一并使用正则表达式处理；
# #最后将标题前后的括号进行去除

# #第一步使用正则表达式r"(.?)|{.?}|[.*?]"匹配并删除字符串中间括号内的内容,如(xxx)、{xxx}、[xxx]

# #第二步使用正则表达式r"(.?|\{.*?∣{.∗?|[.?$"匹配并删除字符串结尾括号内的内容,如(xxx、{xxx}、[xxx

# #第三步使用正则表达式r")|]|}"匹配并删除字符串结尾的右括号,如)】}

# def remove_bracket(series:pd.Series):
#     series = series.apply(
#         lambda c: re.sub(r"\(.*?\)|\{.*?}|\[.*?]", "" , c)
#         )
#     series = series.apply(
#         lambda c: re.sub(r"\(.*?$|\{.*?$|\[.*?$", "" , c)
#         )
#     series = series.apply(
#         lambda c: re.sub(r"\)|\]|\}", "", c)
#     )
#     return series


# def remove_useless_characters(series:pd.Series):
#     series = series.apply(lambda c: c.encode('utf-8').decode())
#     series = series.apply(
#         lambda c: re.sub(r"‘|’|”|“", "'", c).strip()
#     )
#     series = series.apply(
#         lambda c: re.sub(r"\n|\r|'\n|'\r", "", c).strip()
#     )
#     series = series.apply(
#         lambda c: c.strip()
#     )
#     return series
#对电影进行合并，主要依据是title的相似度
#在合并的时候，将genre、actor、director、format、language一起合并，合并依据与title相似
#release date、first available date、run_time按照小的进行合并
#记录一下血缘关系
#../data/relationship.csv
#../data/merge.csv
from fuzzywuzzy import process
import pandas as pd
import numpy as np
import ast
converters={'genre': ast.literal_eval,
            'actor': ast.literal_eval,
            'director': ast.literal_eval,
            'format': ast.literal_eval,
            'language': ast.literal_eval,}
df_merge=pd.read_csv("D:\DataWareHouse\data\movies-reptile.csv",converters=converters,index_col=0)[0:10000]
columns=['asin','genre','release_date','actor','director','format','run_time','language']
#title作为主键
df_relationship=pd.DataFrame(columns=columns)



def add_relationships(column_name:str,title:str,asin:str):
    if column_name in('genre','actor','director','format','language'):
        if asin not in df_relationship.loc[title,column_name]:
            df_relationship.loc[title,column_name].append(asin)
    elif column_name in ('release_date','run_time'):
        df_relationship.loc[title,column_name]=asin


def merge_people_and_others(match_index,choice_list,column_name,title,asin):
    match_list=df_merge.loc[match_index][column_name]
    for choice in choice_list:
        if match_list==[]:
            match_list.append(choice)
            add_relationships(column_name,title,asin)
            continue
        
        _,score=process.extractOne(choice,match_list)
        #是同一个
        if(score>95):
            pass
        #不是同一个
        else:
            match_list.append(choice)
            add_relationships(column_name,title,asin)
          
    return match_list
    


def merge_time_and_date(match_index,choice_time,column_name,title,asin):
    match_time=df_merge.loc[match_index][column_name]
    if not pd.isnull(match_time) and not pd.isnull(choice_time):
        min_time_or_date=min(match_time,choice_time)
        if min_time_or_date==choice_time:
            add_relationships(column_name,title,asin)
        df_merge.loc[match_index,column_name]=min_time_or_date
    elif pd.isnull(match_time) and not pd.isnull(choice_time):
        add_relationships(column_name,title,asin)
        df_merge.loc[match_index,column_name]=choice_time
    

def merge_asin(title,asin):
    df_relationship.loc[title,'asin'].append(asin)



#修改df_merge && df_relationship
def merge(choice_series:pd.Series,match_index,match_title_name:str)->None:
    asin=choice_series['asin']

    merge_asin(match_title_name,asin)
    for col in("genre","actor","director","format","language"):
        merge_people_and_others(match_index,choice_series[col],col,match_title_name,asin)

    for col in('release_date','run_time'):
        merge_time_and_date(match_index,choice_series[col],col,match_title_name,asin)




# ['asin','genre','release date','actor','director','format','run time','language']
def new_df_relationship(title:str,series:pd.Series):
    df_relationship.loc[title]=[[],[],np.nan,[],[],[],np.nan,[]]

    for column_name in('asin','genre','actor','director','format','language'):
    
        if series[column_name]!=[]:
            df_relationship.loc[title,column_name].append(series['asin'])
    

    for column_name in('release_date','run_time'):
        if not pd.isnull(series[column_name]):
            df_relationship.loc[title,column_name]=series['asin']

    



title_pool={}
delete_index=[]
# 假设df是你的DataFrame对象
# 在这里对每一行进行操作
# index是行索引,row是一个Series对象包含该行的数据
# 你可以通过row['column_name']获取单元格值
#'asin','genre','release date','actor','director','format','run time','language'
i=0
for index, row in df_merge.iterrows():
    i+=1
    print(i)
    #新的电影
    if title_pool=={}:
        title_pool[index]=row['title']
        new_df_relationship(row['title'],row)
        continue

    match_title_name,score,match_index=process.extractOne(row['title'],title_pool)
    #大于95的默认是一部电影
    if(score>95):
        merge(row,match_index,match_title_name)
        delete_index.append(index)
    #新的电影
    else:
        title_pool[index]=row['title']
        new_df_relationship(row['title'],row)



df_merge.drop(labels=delete_index,inplace=True)
df_merge.to_csv("movies-after-merge1111.csv",header=True)
df_relationship.to_csv("movies-relationship1111.csv",header=True)

