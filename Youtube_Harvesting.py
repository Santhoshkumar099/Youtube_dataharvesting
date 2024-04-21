#import some requirements
import googleapiclient.discovery
import pandas as pd
import mysql.connector
import datetime
import sqlalchemy
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu




api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyBpEJPMGkC_hoZ1Q-3U0x5johH4ZZyDbOE"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#To get Channel details using channel id
def channel_data(channel_id):

    request = youtube.channels().list(
            part="snippet,contentDetails,status,statistics",
            id=channel_id
        )
    response= request.execute()

    for i in response.get('items',[]):
        data={
            "channel_name":i['snippet']['title'],
            "channel_vidcount" : i['statistics']['videoCount'],
            "channel_subcount" : i['statistics']['subscriberCount'],
            "channel_views" : i['statistics']['viewCount'],
            "channel_Id" :channel_id ,
            "Playlist_Id" : i['contentDetails']['relatedPlaylists']['uploads'],
            "channel_type" :i['kind'],
            "channel_description" :i['snippet']['description'],
            "Channel_status" : i['status']['privacyStatus']
            }
        return data


#To get Video ids by using channel id
def channel_data1(channel_id):
    video_ids = []
    request = youtube.channels().list(id=channel_id,    
                                part='contentDetails')
    resource=request.execute()

    Playlist_Id = resource['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page = None

    while True:
        request = youtube.playlistItems().list(playlistId=Playlist_Id,
                                            part='snippet',
                                            maxResults=50,
                                            pageToken=next_page)
        resource=request.execute()
        
        for i in range(len(resource['items'])):
            video_ids.append(resource['items'][i]['snippet']['resourceId']['videoId'])
        next_page = resource.get('nextPageToken')

        if next_page is None:
            break
    return video_ids,Playlist_Id


#To get video details by video ids
def video_details(video_ids,playlistId):
    video_stats = []

    for Video_info in video_ids:
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics,status",
            id=Video_info).execute()
        
        #To change duration time from ISO to Seconds
        def time_duration(t):    
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b
        for video in response['items']:
            video_details = { "Video_id" : video['id'],
                                "Playlist_Id" : playlistId,
                                "Video_name" : video['snippet']['title'],
                                "Video_Description" : video['snippet']['description'],
                                "Published_date" : video['snippet']['publishedAt'],
                                "View_count" : video['statistics']['viewCount'],
                                "Like_count" : video['statistics'].get('likeCount'),
                                "Favorite_count" : video['statistics']['favoriteCount'],
                                "Comment_count" : video['statistics'].get('commentCount'),
                                "Duration" : time_duration(video['contentDetails']['duration']),
                                "Thumbnail" : video['snippet']['thumbnails']['default']['url'],
                                "Caption_status" : video['contentDetails']['caption']
                                    }
            video_stats.append(video_details)
    return video_stats



#To get comment details by using video ids
def comment_details(video_ids):
        comment_detail=[]
        try:
                for j in video_ids:
                        request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=j,
                        maxResults=100)
                        response=request.execute()

                        for i in response['items']:
                                data1 ={"comment_id" : i['snippet']['topLevelComment']['id'],
                                        "Video_id"  : i['snippet']['topLevelComment']['snippet']['videoId'],
                                        "comment_text" : i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        "author_name" : i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        "Comment_Published" : i['snippet']['topLevelComment']['snippet']['publishedAt'],
                                                }
                                comment_detail.append(data1)
        except:
                pass
        return comment_detail 



#call function
def finaldata(channel_id):
    Channel_Details=channel_data(channel_id)
    Video_Ids,Playlist_Id = channel_data1(channel_id)
    Video_Details=video_details(Video_Ids,Playlist_Id)
    Comment_information=comment_details(Video_Ids)

    final = {
                "channel":Channel_Details,
                "videoid":Video_Ids,
                "video":Video_Details,
                "comment":Comment_information
                }
    return final


#MYSQL Connection
mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
mycursor.execute('CREATE DATABASE IF NOT EXISTS Project_01')
mycursor.execute('use Project_01')
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",
                                                                                pw="",
                                                                                db="Project_01"))


#To create a channel Table
mycursor.execute("""CREATE TABLE IF NOT EXISTS channel (
            channel_ID VARCHAR(255) UNIQUE KEY,
            channel_name VARCHAR(255),
            channel_type VARCHAR(255),
            Playlist_Id  VARCHAR(255),
            channel_vidcount  BIGINT,
            channel_subcount  BIGINT,  
            channel_views BIGINT,
            channel_description TEXT,
            Channel_status VARCHAR(255)
            )""")


#To create a Video table
mycursor.execute("""CREATE TABLE IF NOT EXISTS Video(
                Video_id VARCHAR(255) PRIMARY KEY,
                Video_name VARCHAR(255),
                Playlist_Id  VARCHAR(255),
                Video_Description TEXT, 
                Published_date DATETIME,
                View_count INT,
                Like_count INT,
                Favorite_count INT,
                Comment_count INT,
                Duration TIME,
                Thumbnail VARCHAR(255),
                Caption_status VARCHAR(255)
                )""")




#To create a comment table
mycursor.execute("""CREATE TABLE IF NOT EXISTS comment(
        comment_id VARCHAR(255),
        Video_id VARCHAR(255),
        FOREIGN KEY(Video_id) REFERENCES Video(Video_id),
        comment_text TEXT,
        author_name VARCHAR(255),
        Comment_Published DATETIME
                )""")





#Streamlit Page
page = st.sidebar.selectbox("Select Page", ["Home Page", "SQL Query","Data Analysis" ])



if page == "Home Page":     
    st.header('🎥Youtube data Harvesting and Warehousing') #streamlit Header
    st.subheader('Welcome !')
    channel_id = st.text_input('**Enter the Channel ID**')     
    st.write('(**Collects data** by using :orange[channel id])')
    Get_data = st.button('**Collect Data**')
    if Get_data:
        finaloutput = finaldata(channel_id)
        st.success("Data collected and inserted into the database successfully!")

        channel_df =pd.DataFrame([finaloutput["channel"]],index=[1]) 
        st.dataframe(channel_df)
        video_df = pd.DataFrame(finaloutput["video"])
        comment_df =pd.DataFrame(finaloutput["comment"])
        

        channel_df.to_sql('channel', con=engine, if_exists='append', index=False)
        mydb.commit()

        video_df.to_sql('video',con=engine,if_exists='append',index=False)
        mydb.commit()

        comment_df.to_sql('comment',con=engine,if_exists='append',index=False)
        mydb.commit()


if page == "SQL Query":

    question_tosql = st.selectbox('Select your Question]',
                            ['1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes  for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'],
                                key='collection_question')


    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        #What are the names of all the videos and their corresponding channels?":
        mycursor.execute("""SELECT video.Video_name,channel.channel_name
        FROM video
        INNER JOIN channel ON Video.playlist_id = channel.playlist_id""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)

    if question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute(""" SELECT channel.Channel_Name, COUNT(video.Video_ID) AS Video_Count
        FROM channel
        INNER JOIN video ON channel.Playlist_Id = video.Playlist_Id
        GROUP BY channel.Channel_Name
        ORDER BY Video_Count DESC""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)

    if question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        #What are the top 10 most viewed videos and their corresponding channels?
        mycursor.execute("""SELECT channel.Channel_Name,video.Video_Name,video.View_Count
        FROM video
        INNER JOIN channel ON video.Playlist_Id = channel.Playlist_Id
        ORDER BY video.View_Count DESC
        LIMIT 10""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)


    if question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        #How many no.of comments on each video and their corresponding video names?
        mycursor.execute(""" SELECT video.Video_Name,  COUNT(*) AS CommentCount
                        FROM video
                        INNER JOIN comment ON video.Video_ID = comment.Video_ID
                        GROUP BY video.Video_Name""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)
            
    if question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        #Which videos have the highest no.of likes and their corresponding channel names?
        mycursor.execute(""" SELECT channel.Channel_Name,video.Video_Name,video.Like_Count
                        FROM video
                        INNER JOIN channel ON video.Playlist_Id = channel.Playlist_Id
                        ORDER BY video.Like_Count DESC""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)
        
    if question_tosql == '6. What is the total number of likes  for each video, and what are their corresponding video names?':
        # What is the total no.of likes for each video and their corresponding video names?
        mycursor.execute("""SELECT video.Video_Name, MAX(video.Like_Count) AS Total_Likes
                        FROM video
                        GROUP BY video.Video_Name""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)

    if question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        # What is the total no.of views for each channel and their corresponding channel names?
        mycursor.execute("""SELECT channel.Channel_Name, SUM(video.View_Count) AS Total_Views
                FROM channel
                INNER JOIN video ON Channel.Playlist_Id = Video.Playlist_Id
                GROUP BY Channel.Channel_Name""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)
    
    if question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        # What are the names of all the channels that published videos in the year 2022?
        mycursor.execute("""SELECT Channel.Channel_Name,Video.Video_Name
                FROM Channel
                INNER JOIN Video ON Channel.Playlist_Id = Video.Playlist_Id
                WHERE YEAR(Video.Published_Date) = 2022""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)
    
    if question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        #What is the average duration of all videos in each channel?
        mycursor.execute("""SELECT Channel.Channel_Name, AVG(TIME_TO_SEC(Video.Duration)) AS AvgDuration_sec
                FROM Channel
                INNER JOIN Video ON Channel.Playlist_Id = Video.Playlist_Id
                GROUP BY Channel.Channel_Name""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)

    if question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        # Which videos have the highest no.of comments and their corresponding channel names?
        mycursor.execute("""SELECT Channel.Channel_Name,Video.Video_Name,Video.Comment_Count
                FROM Video
                INNER JOIN Channel ON Video.Playlist_Id = Channel.Playlist_Id
                ORDER BY Video.Comment_Count DESC""")
        mydb.commit()
        out=mycursor.fetchall()
        st.dataframe(out)

                        #Data Analysis  of youtube channels by using local json file

import altair as alt
import numpy as np
import math
import json

if page == "Data Analysis":
            
    def round_up(n, decimals=0): 
        multiplier = 10 ** decimals 
        return math.ceil(n * multiplier) / multiplier

    # Session state
    if "x_index" not in st.session_state:
        st.session_state.x_index = 1
    if "y_index" not in st.session_state:
        st.session_state.y_index = 0
    if "subscribers_count_scale" not in st.session_state:
        st.session_state.subscribers_count_scale = (0,0)
    if "view_count_scale" not in st.session_state:
        st.session_state.view_count_scale = (0,0)


    st.title('📺 YouTube Channel Data Analytics')


    # Reading the JSON data
    with open('F:\IT Field\Python01\MDTM20\Project01\proj.json') as f:
        json_data = json.load(f)

    df = pd.DataFrame(json_data[1:])
    df = df[df.subscribers_count.str.isnumeric()]
    df.subscribers_count = pd.to_numeric(df.subscribers_count)
    df.view_count = pd.to_numeric(df.view_count)


    # Selectbox
    col1, col2 = st.columns(2)
    options = ['subscribers_count','video_count']
    selected_x = col1.selectbox('Choose a variable for the X-axis', options, index=st.session_state.x_index)
    selected_y = col2.selectbox('Choose a variable for the Y-axis', options, index=st.session_state.y_index)

    if (selected_x == 'subscribers_count') or (selected_y == 'subscribers_count'):
        variable_min = 0
        variable_max = max(df.subscribers_count)
        st.session_state.subscribers_count_scale = st.slider('Select the range values for subscriber_count', variable_min, variable_max, (variable_min, variable_max))
        #df = df[df.subscribers_count<st.session_state.subscribers_count_scale[1]]
        df = df[df.subscribers_count<round_up(st.session_state.subscribers_count_scale[1], -7) ]
        df = df[df.subscribers_count>st.session_state.subscribers_count_scale[0]]


    # Creating the scatter plot

    chart = alt.Chart(df).mark_circle(color='#E74C3C').encode(
        x = selected_x,
        y = selected_y,
        #x = alt.X(selected_x, scale = xscale),
        #y = alt.Y(selected_y, scale = yscale),
        tooltip = ['subscribers_count', 'video_count', 'view_count']
    )

    st.altair_chart(chart, use_container_width=True)

    with st.expander('Show DataFrame'):
        st.write(df)