from googleapiclient.discovery import build
import pandas as pd
import pymongo
import mysql.connector
import datetime
import re
import streamlit as st


def connect_to_youtube_api():
    Api_key ="AIzaSyCqpXMMR4uUG2xDEg3sjYoB0gcjyTEQYjg"

    api_service_name="youtube"
    api_version     ="v3"
    youtube         =build(api_service_name,api_version,developerKey=Api_key)

    return youtube

youtube=connect_to_youtube_api()


#Get channels information

#Defining the function 
def get_channel_info(channel_id):
    all_data         = []       
#get channels credentials
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id  = channel_id
    )
    response= request.execute()
#print(response)
#for loop through items
    for item in response["items"]:
        data = {
            "Channel_Name"       : item["snippet"]["title"],
            "Channel_Id"         : item["id"],
            "Subscription_Count" : item["statistics"]["subscriberCount"],
            "Channel_Views"      : item["statistics"]["viewCount"],
            "Total_Videos"       : item["statistics"]["videoCount"],
            "Playlist_id"        : item["contentDetails"]["relatedPlaylists"]["uploads"],
            "Channel_description":item["snippet"]["description"]
        }
        all_data.append(data)

    return all_data


#Get playlists Information 

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part       ="snippet,contentDetails",
            channelId  =channel_id,
            maxResults =50,
            pageToken  =next_page_token
        )
        response = request.execute()

        for item in response["items"]: 
            data={
                "PlaylistId"  :item["id"],
                "Title"       :item["snippet"]["title"],
                "ChannelId"   :item["snippet"]["channelId"],
                "Channel_Name":item["snippet"]["channelTitle"],
                "PublishedAt" :item["snippet"]["publishedAt"],
                "VideoCount"  :item["contentDetails"]["itemCount"]
            }
            All_data.append(data)

        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break

    return All_data


#get video-Ids Information

def get_videos_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(
        id   = channel_id,
       part  ="contentDetails").execute()
      
    
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
  
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part       ="contentDetails",
            playlistId =playlist_Id,
            maxResults =50,
            pageToken  =next_page_token).execute()
        
        for i in response1["items"]:
            video_ids.append(i["contentDetails"]["videoId"])
        next_page_token = response1.get("nextPageToken")
       

        if not next_page_token :
            break
        
    return video_ids
#video_Ids=get_videos_ids(channel_id)


#Get videos information

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id  =video_id,
        )
        response=request.execute()

        for item in response["items"]:
           data ={
                "Video_Id"      : item["id"],
                "Channel_Name"  : item["snippet"]["channelTitle"],
                "Channel_Id"    : item["snippet"]["channelId"],
                "Title"         : item["snippet"]["title"],
                "Thumbnail"     : item["snippet"]["thumbnails"]["default"]["url"],
                "description"   : item["snippet"]["description"],
                "PublishedAt"   : item["snippet"]["publishedAt"],
                "Duration"      : item["contentDetails"]["duration"],
                "Definition"    : item["contentDetails"]["definition"],
                "View_Count"    : item["statistics"].get("viewCount"),
                "Like_Count"    : item["statistics"].get("likeCount"),
                "Comment_count" : item["statistics"].get("commentCount"),
                "Favorite_Count": item["statistics"].get("favoriteCount"),
                "Caption_Status": item["contentDetails"]["caption"]
            }
        video_data.append(data)
        
    return video_data


#Get comment information

def get_comment_info(video_ids):
    comment_data=[]
    
    for video_id in video_ids:
        request  = youtube.commentThreads().list(
            part       ="snippet",
            videoId    =video_id,
            maxResults =50
        )
        response=request.execute()

        for item in response["items"]:
            data = {
                'Comment_Id'    : item["snippet"]["topLevelComment"]["id"],
                'Video_Id'      : item["snippet"]["videoId"],
                'Comment_Text'  : item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                'Comment_Author': item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                'Comment_PublishedAt': item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
            comment_data.append(data)
   
    return comment_data


#Upload and Create database in mongodb
client=pymongo.MongoClient("mongodb://localhost:27017")
db    =client["youtube"]

#Upload channel details to mongodb
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vid_ids    = get_videos_ids(channel_id)
    video_dt   = get_video_info(vid_ids)
    comment_dt = get_comment_info(vid_ids)

    collection1 = db["channel_details"]
    collection1.insert_one({
                            "channel_information" :ch_details,
                            "playlist_information":pl_details,
                            "video_information"   :video_dt,
                            "comment_information" :comment_dt
                            })
    return "uploaded successfully in mongoDB"
#channel_details("UCJZ7zr9a6AT6STkyOFztgiQ")
    

def channels_table():
    mydb = mysql.connector.connect(                            
    host="localhost",
    user="root",
    password="Buffin@12345",
    database="youtube",
    port="3306"
    )

    cursor=mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query ='''CREATE TABLE IF NOT EXISTS channels(Channel_Name varchar(255),
                                                            Channel_Id varchar(255) primary key,
                                                            Subscription_Count int,
                                                            Channel_Views bigint,
                                                            Total_Videos int,
                                                            Playlist_id varchar(255),
                                                            Channel_description text)'''
    
        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")
        
    channel_list=[]
    db=client["youtube"]
    collection1 =db["channel_details"]


    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        channel_list.extend(ch_data["channel_information"])

    df_channel= pd.DataFrame(channel_list)

    for index,row in df_channel.iterrows():
        insert_query = '''INSERT INTO channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Channel_Views,
                                            Total_Videos,
                                            Playlist_id,
                                            Channel_description)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Total_Videos'],
                row['Playlist_id'],
                row['Channel_description'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")
        except Exception as e:
            print(f"Error inserting data: {e}")
            

def playlists_table():
    mydb = mysql.connector.connect(                            
    host="localhost",
    user="root",
    password="Buffin@12345",
    database="youtube",
    port="3306"
    )
    cursor=mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    
    
    try:
        create_query ='''CREATE TABLE IF NOT EXISTS playlists(PlaylistId varchar(255),
                                                                Title varchar(255),
                                                                ChannelId varchar(255),
                                                                Channel_Name varchar(255),
                                                                PublishedAt timestamp,
                                                                VideoCount int )'''
        
        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")
        

    play_list=[]
    db=client["youtube"]
    collection1 =db["channel_details"]


    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        play_list.extend(pl_data["playlist_information"])
        #for i in range(len(pl_data["playlist_information"])):
           # play_list.append(pl_data["playlist_information"][i])
        
    df_playlist= pd.DataFrame(play_list)
    
    for index,row in df_playlist.iterrows():
        published_at_str = row['PublishedAt']
        published_at_obj = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")
        
        insert_query = '''INSERT INTO playlists(PlaylistId,
                                                Title,
                                                ChannelId,
                                                Channel_Name,
                                                PublishedAt,
                                                VideoCount)
                                                values(%s,%s,%s,%s,%s,%s)'''

        values=(row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['Channel_Name'],
                published_at_obj,
                row['VideoCount'])
    
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            


def videos_table():
    mydb = mysql.connector.connect(                            
        host="localhost",
        user="root",
        password="Buffin@12345",
        database="youtube",
        port="3306"
    )

    cursor=mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS videos"
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_query ='''CREATE TABLE IF NOT EXISTS videos(Video_Id varchar(255) primary key,
                                                            Channel_Name varchar(255),
                                                            Channel_Id varchar(255),
                                                            Title varchar(255),
                                                            Thumbnail varchar(255),
                                                            description text,
                                                            PublishedAt timestamp,
                                                            Duration int,
                                                            Definition varchar(255),
                                                            View_Count bigint,
                                                            Like_Count int,
                                                            Comment_count int,
                                                            Favorite_Count int,
                                                            Caption_Status varchar(255))'''

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")

        
    video_list=[]
    db=client["youtube"]
    collection1 =db["channel_details"]


    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        video_list.extend(vi_data["video_information"])

    df_video = pd.DataFrame(video_list)
    
    
    for index,row in df_video.iterrows():
        published_at_str = row['PublishedAt']
        published_at_obj = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")
        
        duration_str = row['Duration']
        duration_in_seconds = int(re.findall(r'\d+', duration_str)[0])
        
        insert_query = '''INSERT INTO videos(
                            Video_Id,
                            Channel_Name,
                            Channel_Id,
                            Title,
                            Thumbnail,
                            description,
                            PublishedAt,
                            Duration,
                            Definition,
                            View_Count,
                            Like_Count,
                            Comment_count,
                            Favorite_Count,
                            Caption_Status
                            )
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Video_Id'],
                row['Channel_Name'],
                row['Channel_Id'],
                row['Title'],
                row['Thumbnail'],
                row['description'],
                published_at_obj,
                duration_in_seconds,
                row['Definition'],
                row['View_Count'],
                row['Like_Count'],
                row['Comment_count'],
                row['Favorite_Count'],
                row['Caption_Status'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")
                
        except Exception as e:
            print(f"Error inserting data: {e}")
    
    
def comments_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Buffin@12345",
        database="youtube",
        port="3306"
    )

    cursor = mydb.cursor()

    # Drop table if it exists
    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)
    mydb.commit()

    # Create table if it doesn't exist
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments(
                                                            Comment_Id varchar(255) primary key,
                                                            Video_Id varchar(255),
                                                            Comment_Text text,
                                                            Comment_Author varchar(255),
                                                            Comment_PublishedAt timestamp)'''

        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")

    # Insert comments data
    comment_list = []
    db = client["youtube"]
    collection1 = db["channel_details"]

    for com_data in collection1.find({}, {"_id": 0, "comment_information": 1}):
        comment_list.extend(com_data["comment_information"])

    df_comment = pd.DataFrame(comment_list)

    for index, row in df_comment.iterrows():
        # Convert the Comment_PublishedAt value to a compatible format
        published_at_str = row['Comment_PublishedAt']
        published_at_obj = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")

        # Prepare the insert query
        insert_query = '''INSERT INTO comments(Comment_Id,
            Video_Id,
            Comment_Text,
            Comment_Author,
            Comment_PublishedAt)
            VALUES (%s, %s, %s, %s, %s)'''

        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            published_at_obj
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")

        except Exception as e:
            print(f"Error inserting data: {e}")
            

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "Tables Created Successfully"



#Display the table of channel information in streamlit web application
def Display_channels_table():
    channel_list=[]
    db=client["youtube"]
    collection1 =db["channel_details"]


    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        channel_list.extend(ch_data["channel_information"])

    df_channel= st.dataframe(channel_list)
    

#Display the table of playlist information in streamlit web application
def Display_playlists_table():
    play_list=[]
    db=client["youtube"]
    collection1 =db["channel_details"]


    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        play_list.extend(pl_data["playlist_information"])
        
    df_playlist= st.dataframe(play_list)


#Display the table of video information in streamlit web application
def Display_videos_table():
    video_list=[]
    db=client["youtube"]
    collection1 =db["channel_details"]


    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        video_list.extend(vi_data["video_information"])

    df_video = st.dataframe(video_list)


#Display the table of comment information in streamlit web application
def Display_comments_table():
    comment_list=[]
    db = client["youtube"]
    collection1 =db["channel_details"]


    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        comment_list.extend(com_data["comment_information"])

    df_comment = st.dataframe(comment_list)
        
      
        
#STREAMLIT RUN 

def home_page():
    st.header(":rainbow[Youtube Data Harvesting and Warehousing Project using SQL,MongoDB and Streamlit]",divider='rainbow')

    st.markdown("""
                This project is dedicated to exploring and managing YouTube data efficiently. 
    Whether you're interested in analyzing channel details, extracting valuable insights from videos, or exploring specific inquiries, 
    this platform provides the tools you need.

    **Key Features:**
    - Data Collection: Utilize API integration and Python scripting for efficient data retrieval.
    - Database Management: Seamlessly store and manage data using MongoDB and MySQL.
    - Analytics and Insights: Analyze video details, viewer engagement, and more.

    **Skills Acquired:**
    - Python Scripting
    - Data Collection 
    - MongoDB
    - Streamlit
    - API Integration
    - Data Management using MongoDB and MySQL 

    **Get Started:**
    Choose "Extract and Transform" from the sidebar to begin exploring and collecting YouTube data. 
    Dive into the various tables to view channels, playlists, videos, and comments. 
    When you're ready, navigate to "view" the insights.

    *Happy exploring and analyzing!*
    """)


def extract_and_transform_page():
    st.title("Extract and Transform Data")
    channel_id = st.text_input("**Enter Youtube Channel_ID below 👇**")

    if st.button("Collect and Store Data"):
        ch_ids = []
        db = client["youtube"]
        collection1 = db["channel_details"]

        for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
            for channel_info in ch_data["channel_information"]:
                ch_ids.append(channel_info["Channel_Id"])

        if channel_id in ch_ids:
            st.success("Channel details of the given channel id: " + channel_id + " already exist")
        else:
            output = channel_details(channel_id)
            st.success(output)

    if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)

    Display_table = st.radio("**SELECT THE TABLE FOR VIEWING 👇**",
                            (":rainbow[CHANNELS]", 
                             ":red[PLAYLISTS]", 
                             ":violet[VIDEOS]", 
                             ":blue[COMMENTS]"),
                            captions=["Diverse Voices, One Platform.", "Themed Video Journeys.",
                                      "Dive into Video Wonderland.", "Community Voices."], index=None)
    
    st.write("**You selected:**", Display_table)

    if Display_table == ":rainbow[CHANNELS]":
        Display_channels_table()
    elif Display_table == ":red[PLAYLISTS]":
        Display_playlists_table()
    elif Display_table == ":violet[VIDEOS]":
        Display_videos_table()
    elif Display_table == ":blue[COMMENTS]":
        Display_comments_table()
        

def view_page():
    st.title("Select any questions to get Insights")
    
#MySQL connection and question selection code here
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Buffin@12345",
        database="youtube",
        port="3306"
    )
    cursor = mydb.cursor()

    question = st.selectbox('**Questions**',
                            ('1. Names of all the Videos and their Channel?',
                             '2. Channels with most No.of.Videos and display the counts?',
                             '3. Top 10 most viewed videos and their Channels?',
                             '4. Comments in each video?',
                             '5. Videos with highest likes?',
                             '6. Likes of all videos?',
                             '7. Views of each channel?',
                             '8. Videos published in the year 2022?',
                             '9. Average duration of all videos in each channel?',
                             '10. Videos with the highest number of comments?'))

    
# Query for the above 10 questions using Nested IF 
    if question == '1. Names of all the Videos and their Channel?':
        query1 = '''select Title as videos, Channel_Name as ChannelName from videos'''
        cursor.execute(query1)
        t1 = cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

    elif question == '2. Channels with most No.of.Videos and display the counts?':
        query2 = '''select Channel_Name as ChannelName,Total_Videos as No_of_Videos from channels 
                        order by No_of_Videos desc'''
        cursor.execute(query2)
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"]))
        df_question2 = pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"])
        st.bar_chart(df_question2.set_index("Channel Name"))

        
    elif question == '3. Top 10 most viewed videos and their Channels?':
        query3 = '''select Title as VideoTitle, Channel_Name as ChannelName, View_count as Views from videos
                        order by Views desc limit 10'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Video Title", "Channel Name", "Views"]))
        
    elif question == '4. Comments in each video?':
        query4 = '''select Title as VideoTitle,Channel_Name as ChannelName,Comment_count as No_comments from videos 
                        where Comment_count is  not null order by no_comments desc'''
        cursor.execute(query4)
        t4 = cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Title", "Channel Name", "Comments"]))
        
    elif question == '5. Videos with highest likes?':
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Like_Count as Likes from videos
                        order by Likes desc limit 10'''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Video Title", "Channel Name", "Likes"]))
        
    elif question == '6. Likes of all videos?':
        query6 = '''select Title as VideoTitle, Channel_Name as ChannelName, Like_count as Likes from videos'''
        cursor.execute(query6)
        t6 = cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Video Title", "Channel Name", "Likes"]))
        
    elif question == '7. Views of each channel?':
        query7 = '''select Channel_Name as ChannelName, Channel_Views as TotalViews from channels 
                        order by TotalViews desc '''
        cursor.execute(query7)
        t7 = cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name", "Total Views"]))
        
    elif question == '8. Videos published in the year 2022?':
        query8 = '''select Title as VideoTitle, Channel_Name as ChannelName, PublishedAt as PublishedDT from videos
                        where year(PublishedAt) = 2022'''
        cursor.execute(query8)
        t8 = cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Video Title", "Channel Name", "Published D&T"]))
                
    elif question == '9. Average duration of all videos in each channel?':
        query9 = '''select Channel_Name as ChannelName, avg(Duration) as AverageDuration from videos
                        group by Channel_Name'''
        cursor.execute(query9)
        t9 = cursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel Name", "Average Duration"]))
        
    elif question == '10. Videos with the highest number of comments?':
        query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comment_count as comments from videos 
                        order by Comments desc limit 10'''
        cursor.execute(query10)
        t10 = cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Video Title", "Channel Name", "Comments"]))
        
        
# Create a sidebar container
sidebar = st.sidebar

# Add a header to the sidebar
sidebar.header(":red[Welcome to Youtube Data Harvesting and Warehousing]",divider="rainbow")

# Add a list of items to the sidebar
sidebar_items = ["Home", "Extract and Transform", "View"]
selected_item = sidebar.selectbox(":black[**Select an item:**]", sidebar_items)

# Display the appropriate page based on the selected item
if selected_item == "Home":
    home_page()
elif selected_item == "Extract and Transform":
    extract_and_transform_page()
elif selected_item == "View":
    view_page()

# Add a button to the sidebar
sidebar.button("Submit")

        
