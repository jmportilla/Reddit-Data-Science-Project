__author__ = 'marci'

#### FOR LOCAL TESTING
#pathway = "/Volumes/Seagate Backup Plus Drive 1/RC_2015-01"
pathway = "/Users/marci/Desktop/RC_2015-01"



# DataBase Imports
import json
import pymongo
import itertools 

# NLP Imports
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer


# Doc2Vec Imports
from gensim.models import Doc2Vec
from gensim.models.doc2vec import LabeledSentence
import gensim.models.doc2vec


import multiprocessing

def text_cleaner(text):
	
    '''
    INPUT: string of body text
    OUTPUT: List of tokenized lower case words with stopwords removed
    '''

    # Set up Stop Words
    stop_words = stopwords.words('english')
    stop_words.append('[deleted]')
    tokenizer = RegexpTokenizer(r'\w+')
    
# Output tokenizes text and removes any stopwords and then outptus lowercased words
    return [word.lower() for word in tokenizer.tokenize(text) if not word.lower() in stop_words]



def update_mongo(collection,subreddit,comment):
    '''
    INPUT: subreddit name and comment as a cleaned list of words
    RESULT: Updates MongoDB reddit_data database with $push
    '''
    
    # NOTE: This pushes the entire list, so final result is a list of comment lists
    # Use itertools.chain.from_iterable(reddit_data[subreddit]) to combine this for doc2vec
    collection.update({'subreddit': subreddit},
                      {'$push':{'body':comment}},upsert = True,
                      safe=True)

def clear_mongo(mongodb):
    '''
    INPUT: A mongodb database
    RESULT: clears database but has a user input safety feature.
    '''
    ans = raw_input('WARNING: THIS WILL CLEAR THE DATABASE. ARE YOU SURE? y/n')
    if ans == 'y':
        mongodb.remove()
        print 'Database has been cleared.'
    else:
        print 'Database not cleared.'



def get_doc_list(db_collection,subreddit):
    '''
    INPUT: Subreddit name
    OUTPUT: A document = list of words for that subreddit
    '''
    
    for obj in db_collection.find({'subreddit':subreddit}):
        return list(itertools.chain.from_iterable(obj['body']))


def create_or_connect(database,collection):
    '''
    INPUT: Database name (str) and collection (name)
    OUTPUT: Either creates the database or connects to it if it already exists.
            Returns the collections in the database cursor.
    '''
    
    return pymongo.Connection()[database][collection]


############################
############################


'''
FUNCTIONS BELOW USE DATA TO AND FROM MONGO DB, MAY BE NOT BE NEEDED
'''

###########################
###########################

# Make or Connect to Database

def reddit_data_pusher(db_name,num_of_comments,pathway):
    
    '''
    Goes through the txt file, grabs the JSON objects in it.
    Uses MongoDB to create a database, with key= Subreddit and continually appending clean text word list to value.
    '''
    
    # Set counter
    count = 0
    
    # Iterate through N JSON objects in file
    with open(pathway) as myfile:
        for item in myfile:
            if count < num_of_comments:
                
                # Load the JSON object
                json_object = json.loads(item)
                
                # Clean and tokenize text
                body = text_cleaner(json_object['body'])
                
                # $push to MongoDB
                update_mongo(reddit_data,json_object['subreddit'],body)
                
                # For safety puposes
                count += 1
            else:
                break

def sentence_gen(database):
    for item in database.find():
        words=list(itertools.chain.from_iterable(item['body']))
        yield LabeledSentence(words,labels=[str(item['subreddit'])])

##################################
##################################
"""
FUNCTIONS BELOW READ DIRECTLY FROM DATA AS GENERATOR

"""
#################################
################################

def reddit_comment_gen(pathway,num_of_comments,everything=False):
    '''
	INPUT: Pathway to database and num of comments to be generated. If everything is True, all comments returned.
	OUTPUT: Generator label and tokenized comment

	'''

    ## Generate all labeled sentences from file

    if everything:
        # Iterate through N JSON objects in file
        with open(pathway) as myfile:
            for item in myfile:
	                
                # Load the JSON object
                json_object = json.loads(item)

                # Clean and tokenize text
                body = text_cleaner(json_object['body'])

                # $push to MongoDB
                yield LabeledSentence(body,labels=[str(json_object['subreddit'])])
	               
    ## Generate set number of comments from file
    else:
        # Set counter
        count = 0

        # Iterate through N JSON objects in file
        with open(pathway) as myfile:
            for item in myfile:
                if count < num_of_comments:

                    # Load the JSON object
                    json_object = json.loads(item)

                    # Clean and tokenize text
                    body = text_cleaner(json_object['body'])

                    # For safety puposes
                    count += 1

                    # $push to MongoDB
                    yield LabeledSentence(body,labels=[str(json_object['subreddit'])])


                else:
                    break



def build_model():

    cores = multiprocessing.cpu_count()
    assert gensim.models.doc2vec.FAST_VERSION > -1

    # Doc2Vec(dbow,d100,n5,mc2,t8)
    d2v_reddit_model = Doc2Vec( dm=0, size=100, negative=5, hs=0, min_count=2, workers=cores)
    d2v_reddit_model.build_vocab(reddit_comment_gen(pathway,100)) #sentence_gen(reddit_data))


    #d2v_reddit_model.train(reddit_comment_gen(pathway,10000)) #sentence_gen(reddit_data))

    for epoch in range(10):

        d2v_reddit_model.train(reddit_comment_gen(pathway,100))
        d2v_reddit_model.alpha -= 0.002  # decrease the learning rate
        d2v_reddit_model.min_alpha = d2v_reddit_model.alpha  # fix the learning rate, no decay

    return d2v_reddit_model

if __name__ == "__main__":
    print 'whoo!'
    model = build_model()
    print model.most_similar("AskReddit")















