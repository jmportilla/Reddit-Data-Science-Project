'''
THIS SCRIPT IS TO BE RUN ON A REMOTE AWS SERVER AND WILL DO THE FOLLOWING:


1.) Connect to an S3 instance and download the Reddit Comment Data
2.) Cycle through the data and


# There are 53,851,542 comments.

# Should I create documents??
# Installing C-compiler on instance?
# Installing a BLAS

DEPENDENCIES:

General Anaconda

python-cjson
gensim 0.12
nltk
nltk.download('all-corpora')
'''


# Doc2Vec Imports

import gensim
import gensim.models.doc2vec
from gensim.models.doc2vec import TaggedDocument,LabeledSentence
from gensim.models import Doc2Vec

# Multiprocessing Set-up
import numpy as np
import multiprocessing
cores = multiprocessing.cpu_count()
assert gensim.models.doc2vec.FAST_VERSION > -1


#### NLP Modules
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords


#### AWS Connection Sockets
import boto
from boto.s3.connection import S3Connection


##### Word Cleaner

def text_cleaner(text):
    '''
    INPUT: string of body text
    OUTPUT: List of tokenized lower case words with stopwords removed
    '''
    
    
    # Output tokenizes text and removes any stopwords and then outptus lowercased words
    return [word.lower() for word in tokenizer.tokenize(text) if not word.lower() in stopwords]


##### Generate Labeled Sentence Docs from the Reddit DataSet

def reddit_comment_gen(pathway):
    '''

    INPUT: Pathway to data to be generated.
    OUTPUT: Generator label and tokenized comment

    '''

    ## Generate all labeled sentences from file

    
   
    # Iterate through JSON objects in file
    with open(pathway) as myfile:
        for item in myfile:
            
            
            # put in try statement here

            # Load the JSON object
            json_object = cjson.decode(item)

            # Clean and tokenize text
            body = text_cleaner(json_object['body'])

            # generate
            yield LabeledSentence(body,[str(json_object['subreddit'])])


####################################
#
# MONGO DB FUNCTIONS SECTION
#


import pymongo

# Connect with db
#reddit_data = pymongo.Connection()['reddit_database']['comments']

def create_or_connect(database,collection):
    '''
    INPUT: Database name (str) and collection (name)
    OUTPUT: Either creates the database or connects to it if it already exists.
            Returns the collections in the database cursor.
    '''
    
    return pymongo.Connection()[database][collection]



def clear_mongo(mongodb):
    '''
    INPUT: A mongodb database
    RESULT: Based on user input, clears database.
    '''
    ans = raw_input('WARNING: THIS WILL CLEAR THE DATABASE. ARE YOU SURE? y/n')
    if ans == 'y':
        mongodb.remove()
        print 'Database has been cleared.'
    else:
        print 'Database not cleared.'
        


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

import itertools 

def get_doc_list(db_collection,subreddit):
    '''
    INPUT: Subreddit name
    OUTPUT: A document = list of words for that subreddit
    '''
    
    for obj in db_collection.find({'subreddit':subreddit}):
        return list(itertools.chain.from_iterable(obj['body']))


#####################################


def build_model(pathway):
	'''
	INPUT: Pathway to data
	OUTPUT: Complete Doc2Vec Model
	'''

	# Check to use all cores
    cores = multiprocessing.cpu_count()
    assert gensim.models.doc2vec.FAST_VERSION > -1

    # Set up Model Constraints
    d2v_reddit_model = Doc2Vec( dm=0, size=100, negative=5, hs=0, min_count=2, workers=cores)
    
    # Only train Doc2Vec, not Word2Vec
    d2v_reddit_model.train_words = False
    
    # Build a Vocab
    d2v_reddit_model.build_vocab(reddit_comment_gen(pathway)) 


    print "Done Building Vocabulary!"

    ep_count = 1
    for epoch in range(10):

        d2v_reddit_model.train(reddit_comment_gen(pathway))
        d2v_reddit_model.alpha -= 0.002  # decrease the learning rate
        d2v_reddit_model.min_alpha = d2v_reddit_model.alpha  # fix the learning rate, no decay
        print "Epoch complete" %ep_count


    return d2v_reddit_model




def bucket_retriever():


	# Get connection with keys (may need to input these keys)
	conn = S3Connection(aws_access_key_id='AKIAJE74CVWSMVH3FBCQ',aws_secret_access_key='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')

	# Connect to exsisting bucket
	mybucket = conn.get_bucket('testreddit')

	# List keys in the bucket
	mybucket.list()

	# Use key to grab specific file from bucket
	from boto.s3.key import Key

	k = Key(mybucket)

	k.key = 'RC_2015-01'

	# Save file
	k.get_contents_to_filename('reddit_data')


	final_model = build_model('my_sample')





















