import requests
from datetime import datetime, timedelta
import traceback
import time
import json
import sys
import sqlite3


#Currently the PushShift API is throwing a lot of timeouts and errors. Post data from before Nov 3 hasn't been loaded after a bad migration.
#Keep dates after Nov 3 for the time being.
#reddit.com/r/pushshift for updates

#API has been updated. After = since ; before = until || -new parameters on right
#since - search all posts/comments starting from this date until present time
#until - search all posts/comments from the past until this date


#API base URL. Order of inserterted paramenters: commnet/submission, keyword, subreddit, epoch time
#Change limit # in url to increase amount of returns. Up to 1000
url = "https://api.pushshift.io/reddit/{}/search?limit=1000&order=desc&{}&{}&since="   

title_url="https://api.pushshift.io/reddit/{}/search?ids{}" #search post via ID gathered from commnet in order to get post title

comment_link = "https://reddit.com" #permalink received from API starts from /r/subreddit. Concantenate to this variable to save a complete URL


#Change this number to the # of days prior 
#you want to start the search. Ex: 60= from 60 days ago
period=50


#Gets the current time to be converted to epoch time and used as search period
start_time = datetime.utcnow()
starting_search_date = start_time - timedelta(days=period)
print(f"\nStarting search from date {starting_search_date}\n")


#Subreddits to search
#subreddits=["pokemontcg","retrogaming","gamecollecting","retrogaming"]

subreddits=["pokemontcg","retrogaming","gamecollecting","retrogaming","SNES","N64","NES","Gameboy","retrogameswap","PSX","SegaSaturn","Megadrive"]

#Keywords to query the API with. API search not case sensitive. Change the words to target different comments
search_terms = ["Japanese","Japan","yen","JPY","Japan's","Japans"]


#Connect to an SQLite database
conn = sqlite3.connect('submissions.db')
cursor = conn.cursor()

#Creates a new table per subreddit and prepares the columns for loading data
for subreddit in subreddits:
    cursor.execute('''CREATE TABLE IF NOT EXISTS '''+ subreddit+''' (
                    comment_id TEXT PRIMARY KEY,
                    comment_text TEXT,
                    comment_permalink TEXT,
                    comment_score INT,
                    submission_link TEXT,
                    submission_title TEXT,
                    submission_score INT,
                    submission_date DATE)''')



#Subtracts number of days from current time and converts to epoch time. Ex: 10 for 10 days
def search_period(days):
    number_of_days =days*86400
    current_time=start_time.timestamp()

    search_time=current_time-number_of_days
    return search_time
    

def EpochToDateTime(PostTimeStamp):
    PostDate= datetime.fromtimestamp(PostTimeStamp)
    return PostDate


def send_request(object_type,subreddit,keyword):
    print(f"     Checking API for a response before continuing\n     This should only take a second or two if all is well\n")

    filter_string = f"subreddit={subreddit}"
    keyword_string =f"q={keyword}"
    previous_epoch = int(search_period(period)) #takes the start time and converts it to EPOCH time used by the API. Timestamp is in unix epoch time
    new_url = url.format(object_type, keyword_string, filter_string)+str(previous_epoch) #format string for url. Add in order of {} in above URL
    #print(new_url)
    
    json_text = requests.get(new_url, headers={'User-Agent': "Reddit post parser"}) #makes API call
    #time.sleep(1)  # pushshift has a rate limit, if we send requests too fast it will start returning error messages
    return_code = json_text.status_code #use this to determine is API timeout occurs
    
    #Checks if the return code is 200 or something else. Prints out the error code for lookup
    if return_code !=200:
        return_code = json_text.status_code
        print(f"Error occured calling API for {subreddit}: Server returned code {return_code}")
        #timeout_count+=1
        return True
    else:
        return False

def downloadFromUrl(object_type,subreddit,keyword):
    print(f"Parsing {object_type}s now \n")
    print(f"Searching subreddit: /r/{subreddit}\n")
    print(f"Searching comments containing: {keyword}")
    
    #Sets the subreddit parameter in the base API URL call
    filter_string = f"subreddit={subreddit}"
    keyword_string =f"q={keyword} \n"
   
    #Converts "start_time" to epoch time using 'searh_period' function. 'Timestamp' is in unix epoch time. API uses epoch time
    #Change the # in the "search_period" function parameter to change how far back the search will start.
    previous_epoch = int(search_period(period))
   
    #format string for url. Add in order of {} in above URL variable at the top of the script
    new_url = url.format(object_type, keyword_string, filter_string)+str(previous_epoch) 
    #print(new_url) #For testing purpses to determine the current URL in API call
    
    #makes API call
    json_text = requests.get(new_url, headers={'User-Agent': "Reddit post parser"}) 
    
    # pushshift has a rate limit, if we send requests too fast it will start returning error messages
    time.sleep(1) 
    return_code = json_text.status_code #use this to determine is API timeout occurs
    ################################################

    try:
    
        json_data = json_text.json()
        #print(json_data)
    #except json.decoder.JSONDecodeError: #commenting this out because the error is misleading when the API times out

              
        objects = json_data['data']
        if len(objects) == 0:
            
            print(f"\n No comments matching'{keyword}' were found \n Skipping \n")
            pass
    
        for object in objects:  #This is iterating the comment objects returned in JSON data
        
            if object_type == 'comment':
                    #print(object['body'])
                    try:
                        print(f"\nFound match in post ID: {object['link_id']}\n")
                        cursor.execute('''INSERT OR IGNORE INTO '''+(subreddit)+ ''' VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                (object['id'], object['body'],comment_link+object['permalink'], object['score'], object['link_id'],"","", EpochToDateTime(object['created_utc']))) #link id is ID of submission. Need to query to get title/link
                        #Save data to database. This way if it fails later in the loop not all data is lost
                        conn.commit()
                        
                    except Exception as err:
                            print("\nError saving to database \n")
                            print(traceback.format_exc())
            
            #Will be used to searching titles of posts
            #elif object_type == 'submission':
            #    try:

    except:
        pass
        print(f"\n----Received return code: {return_code} ---- \n")
        print("Error in JSON decoder \n This can happen if the API call times out.\n Check the return code \n \n ")

          
        
max_attempts=4

#Starts a new query for every subreddit and 
for i, subreddit in enumerate(subreddits):
    attempts=0
    progress = len(subreddits) - i
    print(f"\n----------------------------------------------------\n")
    print(f"\n------{progress} remaining subreddits to check----- \n")
    #Checks if the API is down or operating normally. True = return code was not good
    #For now it checks once per subreddit. Will add more robust error checking in the future
    api_timeout = send_request("comment",subreddit,"the")
  
    #Sets the condition for 5 retries on API timeout
    while attempts<max_attempts:
        #The code inside the try condition will retry until attempt==4
        try:
            #If the function did not receive a timeout. False was returned = continue
            if not api_timeout:
            
                for keyword in search_terms:
                    print(f"Current keyword is: {keyword} \n")
            
                    #The meat of the script. Passes the API endpoint type (comment or submission), subreddit, and search term to query
                    downloadFromUrl("comment",subreddit,keyword)
                    
                    #time.sleep(1) 
            else:
                attempts+=1
                print(f"\n API has timed out.\n Retrying /r/{subreddit} in 5 seconds \n")
                #time to wait before retrying
                time.sleep(5)
                continue
        except:
            print("\n Something happened with the 'try' condition.\n")
            print(traceback.format_exc())
            
    else:
        print("API has timed out consecutively 5 times. Best to try again later.")
        break
               


#close database connection
conn.close()

