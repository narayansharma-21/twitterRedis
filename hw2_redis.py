import csv
import redis
import time
import random

# redis connection initilization 
r = redis.Redis(host='localhost', port=6379, db=0)

# post_tweet API function 
def post_tweet(tweet_id, tweet_text):
    # using redis' built-in set command to map the ids to the text 
    r.set(f"tweet:{tweet_id}", tweet_text)

# get_timeline API function
def get_timeline(user_id):
    # creating the members of a user's following through data structure of a set 
    user_following = r.smembers(f"following:{user_id}")
    timeline = []
    for followee_id in user_following:
        tweets = r.lrange(f"tweets:{followee_id}", 0, 9)
        for tweet_id in tweets:
            # getting the key value and appending the tweet to the specified key 
            tweet = r.get(f"tweet:{tweet_id}")
            timeline.append(tweet)
    return timeline

# read_follows API function 
def read_follows():
    # just opens the csv file and reads the entirety of a file and saves it as a set 
    with open("follows.csv", "r") as f:
        header = next(f)
        reader = csv.reader(f)
        for row in reader:
            user_id = int(row[0])
            followee_id = int(row[1])
            r.sadd(f"following:{user_id}", followee_id)

# read_tweets API function
def read_tweets():
    # just opens the csv file and posts the tweet using the post_tweet function depending on the specific id of a tweet 
    with open("tweets.csv", "r") as f:
        reader = csv.reader(f)
        for row in reader:
            tweet_id = int(row[0])
            tweet_text = row[1]
            post_tweet(tweet_id, tweet_text)

# main driver method that runs the performance testing 
def run_performance_test(num_timeline_fetches, num_tweets):
    read_follows()

    start_time = time.time()

    # posts all the tweets based on the id of a post 
    for num_post in range(num_tweets):
        random_tweet_text = f"Random tweet text {num_post}"
        post_tweet(num_post, random_tweet_text)
    post_time = time.time() - start_time

    posts_per_sec = num_tweets / post_time

    print(f"Total time to post {num_tweets} tweets: {post_time:.5f} seconds")
    print(f"Average time to post a tweet: {post_time / num_tweets:.5f} seconds")
    print(f"Tweets posted per second: {posts_per_sec:.0f}")
    
    start_time = time.time()

    # retrieves all the timelines in random order while storing the fetch time of it 
    for num_fetch in range(1, num_timeline_fetches):
        random_user = random.randint(0, num_tweets - 1)
        get_timeline(random_user)
    fetch_time = time.time() - start_time

    fetches_per_sec = num_timeline_fetches / fetch_time

    print(f"Total time to fetch {num_timeline_fetches} timelines: {fetch_time:.5f} seconds")
    print(f"Average time to fetch a timeline: {fetch_time / num_timeline_fetches:.5f} seconds")
    print(f"Timeline fetches per second: {fetches_per_sec:.0f}")




if __name__ == "__main__":
    num_timeline_fetches = 100
    num_tweets = 1000
    run_performance_test(num_timeline_fetches, num_tweets)


