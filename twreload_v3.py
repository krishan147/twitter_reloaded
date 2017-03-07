import datetime
import dateutil.parser # for unix time conversion
import json
import os
import pyodbc
import time
import tweepy
import urllib
from collections import defaultdict
import logging
import sys
import timeit
start = timeit.default_timer()
str_logging_time = str(datetime.datetime.now())
import gspread
from oauth2client.service_account import ServiceAccountCredentials
time_start = datetime.datetime.now()

scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('ENTER PROJECT NAME HERE', scope)
gc = gspread.authorize(credentials)
wks = gc.open("ENTER GOOGLE SHEET NAME").sheet1
wks.update_acell('A11', 'Running...')
wks.update_acell('B12', 'Will populate when process is finished')
wks.update_acell('B13', 'Will populate when process is finished')
wks.update_acell('B14', 'Will populate when process is finished')
wks.update_acell('B15', 'Will populate when process is finished')
wks.update_acell('B16', 'Will populate when process is finished')
wks.update_acell('B17', 'Will populate when process is finished')
wks.update_acell('D11', '')
wks.update_acell('E11', '')

### CONNECT TO DB

database_details = 'DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD'
table_name = 'BRPMEN_POSTS'
address_book_table = 'BRPMEN_AddressBookSocialProfiles'

### GET TOTAL NUMBER OF TWEETS ON THE SYSTEM BEFORE THE PROCESS BEGAN

brpmen = 'DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD'
table_name_total = "BRPMEN_Posts"
cnxn = pyodbc.connect(amvbbdo_brpmen)
cursor = cnxn.cursor()
cursor.execute("SELECT tw_tweet_id FROM "+table_name_total+ " ORDER BY tw_tweet_id DESC")
rows = cursor.fetchall()
cnxn.close()

num_of_tweets_before_running_script = 0
for item in rows:
    if "None" in str(item):
        pass
    else:
        num_of_tweets_before_running_script = num_of_tweets_before_running_script + 1

str_num_of_tweets_before_running_script = str(num_of_tweets_before_running_script)

### TWITTER AUTH DETAILS

consumer_token = "ENTER CONSUMER TOKEN"
consumer_secret = "ENTER CONSUMER SERCRET"
access_token = "ENTER ACCESS TOKEN"
access_secret = "ENTER ACCESS SERCRET"
auth = tweepy.OAuthHandler(consumer_token,consumer_secret)
auth.set_access_token(access_token,access_secret)
api  = tweepy.API(auth)

count = "2000" #TWITTER MAX 2000

### SELECT ALL TWEET IDS TO AVOID ADDING DUPLICATES

cnxn = pyodbc.connect(database_details)
cursor = cnxn.cursor()
cursor.execute("select tw_tweet_id from "+table_name+" WHERE tw_tweet_id IS NOT NULL")
fetch = cursor.fetchall()
with open("twitter_database_of_twitter_ids.txt", "a") as database_of_tweet_ids_append:
    for item in fetch:
        database_of_tweet_ids_append.write(item[0])


# SELECT ALL TWITTER HANDLES TO GRAB POSTS FROM

cursor.execute("select matchKey, country, location, venue_name, uniqueid from "+address_book_table+" WHERE platform LIKE 'tw' ORDER BY last_checked ASC")

num_of_new_tweets = 0
rows = cursor.fetchall()
cnxn.close()

for item in rows: # ANALYSE EACH HANDLE ONE AT A TIME
    time_per_handle1 = time.time()
    if item: # some items in the db are empty - this line will disregard empty elements
        twitter_handle = item[0]
        BRPMEN_AddressBookSocialProfiles_uniqueId = item[4]
        country = item[1]
        city = item[2]
        name = item[3]
        print twitter_handle
        ms_sql_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')      
        time_now = datetime.datetime.now()
        convert_time_now = str(time_now).split(".",1)[0]
        time_now_unix = time.mktime(datetime.datetime.strptime(convert_time_now, "%Y-%m-%d %H:%M:%S").timetuple())
        year_timestamp = 31591981
        day_timestamp = 41210
        time_then = time_now_unix - day_timestamp

        fetch_data_from_twitter_api = tweepy.Cursor(api.user_timeline, id=twitter_handle).items(count)
        try:
            for status2 in fetch_data_from_twitter_api:
                status = status2._json
                    
                tweet_id = status["id"] # tweet ID
                dt = dateutil.parser.parse(status["created_at"]) # convert time to unix time to check how old the tweet is
                tweet_unix_date = int(time.mktime(dt.timetuple())) # unix timestamp of the tweet
                database_format_for_created_time = datetime.datetime.strptime(status["created_at"],'%a %b %d %H:%M:%S +0000 %Y')
                status_id = twitter_handle + "/status/" + str(tweet_id) # custom tweet id for DB
                tweet = status["text"] # tweet text
                twitter_id2 = status["user"]["id"] # user ID
                follower_count = status["user"]["followers_count"]
                
                if tweet_unix_date >= time_then:
                    
                    #print tweet_id, status["created_at"], tweet_unix_date, status_id, tweet
                    
                    original_date = datetime.datetime.now()
                    f = open('twitter_database_of_twitter_ids.txt', 'r')
                    lines = f.read()
                    if status_id not in lines:
                        with open("twitter_database_of_twitter_ids.txt", "a") as database_of_tweet_ids_append: #add the status ID to the database of statuses
                            database_of_tweet_ids_append.write(status_id)

                        print twitter_handle, status_id, city, name, tweet, database_format_for_created_time, country, twitter_id2, time_now

                        print "adding " + status_id + " ...."

                        encode_post = tweet.encode('ascii', 'ignore').decode('ascii')
                        tweet_nogap = encode_post.replace("\n"," ")

                        tw_url = "www.twitter.com/"+status_id

                        time.sleep(1)
                        #try:
                        cnxn = pyodbc.connect(database_details)
                        cursor = cnxn.cursor()
                        cursor.execute("insert into " +table_name+ " (twitter_handle, tw_tweet_id, location, venue_name, tweet, date_posted, country, tw_id, date_added, twitter_followers,tw_url) values (?,?,?,?,?,?,?,?,?,?,?)",
                                       twitter_handle, status_id, city, name, tweet_nogap, database_format_for_created_time, country, twitter_id2, time_now, follower_count,tw_url)
                        cnxn.commit()
                        cnxn.close()
                        num_of_new_tweets = num_of_new_tweets + 1

                        print "tweet " + status_id + " successfully added."

                        #except Exception:
                        #    pass

                    if status_id in lines:
                        print "status already exists: " + status_id
                        #break
                    f.close()

                else:
                    break # break the loop if the tweet is outside of the time range as all the remaining tweet will be outside of range too
                
        except Exception, e:
            with open ('twitter_log.txt', 'w') as error_catch:
                error_catch.write("There was an error: " + str(e) + "\n")

                str_error = str(e)
                wks.update_acell('D11', 'Process finished but there was an error')
                wks.update_acell('E11', str_error)
                
                print "There was an error. Refer to twitter_log.txt for error message."
            error_catch.close()
            #sys.exit()
                        
        # SET LAST CHECKED DATE IN SQL ADDRESS BOOK
        
        cnxn = pyodbc.connect(database_details)
        cursor = cnxn.cursor()
        print ms_sql_time, BRPMEN_AddressBookSocialProfiles_uniqueId
        xyztest = "UPDATE BRPMEN_AddressBookSocialProfiles SET last_checked=%s WHERE uniqueid='%s'" % (ms_sql_time, BRPMEN_AddressBookSocialProfiles_uniqueId)                                               
        print xyztest
        cursor.execute("UPDATE BRPMEN_AddressBookSocialProfiles SET last_checked='%s' WHERE uniqueid='%s'" % (ms_sql_time, BRPMEN_AddressBookSocialProfiles_uniqueId))                                                      
        cnxn.commit()
        cnxn.close()
        print "Successfully added last checked date"

        # CHECK HOW LONG IT TAKES TO RUN ONE HANDLE. IF NECESSARY PAUSE TO AVOID TWITTER THROTTLING
        time_per_handle2 = time.time()
        calculate_run_time = time_per_handle2-time_per_handle1
        print "time per 1 handle: " + str(calculate_run_time) + " for username " + twitter_handle
        if calculate_run_time<60:
            time.sleep(60-calculate_run_time)

os.remove("twitter_database_of_twitter_ids.txt")

### TIMER STOPS
stop = timeit.default_timer()
run_time = stop - start
m, s = divmod(run_time, 60)
h, m = divmod(m, 60)
hms_run_time = "%dh %02dm %02ds" % (h, m, s)

total_num_of_tweets = num_of_tweets_before_running_script + num_of_new_tweets
str_total_num_of_tweets = str(total_num_of_tweets)
str_num_of_new_tweets = str(num_of_new_tweets)

str_time_end = str(datetime.datetime.now())

### LOG ERRORS AND TOP LINE DATA ON HOW THE PROCESS WENT 

with open('twitter_log.txt', 'w') as success_message:
    success_message.write("Script finished successfully!" + "\n")
    success_message.write("Script started at " + str_logging_time + "\n")
    success_message.write("Script stopped at " + str_time_end + "\n")
    success_message.write("This script took " + hms_run_time + " to run" + "\n")
    success_message.write("The number of tweets on the system before the script ran was " + str_num_of_tweets_before_running_script + "\n")
    success_message.write("The number of tweets on the system after the script has ran is now " + str_total_num_of_tweets + "\n")
    success_message.write(str_num_of_new_tweets + " new tweets were added")
    print "Script finished. Please refer to twitter_log.txt for top line stats"
success_message.close()

wks.update_acell('A11', 'Script finished!')
wks.update_acell('B12', str_logging_time)
wks.update_acell('B13', str_time_end)
wks.update_acell('B14', hms_run_time)
wks.update_acell('B15', str_num_of_tweets_before_running_script)
wks.update_acell('B16', str_total_num_of_tweets)
wks.update_acell('B17', str_num_of_new_tweets)

