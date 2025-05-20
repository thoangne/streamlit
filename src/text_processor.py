import re
import string
import nltk
import contractions
import emoji
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import spacy

from .logger_config import setup_logger

# Tải các tài nguyên cần thiết của NLTK
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')

try:
    nltk.data.find('taggers/averaged_perceptron_tagger')
except LookupError:
    nltk.download('averaged_perceptron_tagger')
nltk.data.path.append('C:/Users/Admin/AppData/Roaming/nltk_data')
# Tải mô hình spaCy
nlp = spacy.load("en_core_web_sm")

# Thiết lập logger
logger = setup_logger()

def expand_contractions(text):  
    '''
    Expands contractions in text to full form.

    Example: 
    >>> expand_contractions("I can't do this")
        "I cannot do this"
    '''
    return contractions.fix(text)

def replace_emoji(text):
    '''
    Replace Emoji in text with corresponding text description
    '''
    return emoji.demojize(text).replace("_", " ").replace(":", " ")

def lowercase_text(text):
    '''
    Convert text to lowercase.
    '''
    return text.lower()

def remove_punctuation(text):
    '''
    Remove punctuation from text.
    '''
    return text.translate(str.maketrans('', '', string.punctuation))

def remove_numbers(text):
    '''
    Remove numbers from text.
    '''
    return re.sub(r'\d+', '', text)

def remove_special_characters(text):
    '''
    Remove special characters from text
    '''
    return re.sub(r'[^a-zA-Z0-9\s]', '', text)

def remove_whitespace(text):
    '''
    Remove extra whitespaces from text
    '''
    return text.strip()

def remove_stopwords(text):
    '''
    Remove stopwords from text
    '''
    if not isinstance(text, str) or not text.strip():
        return ""
    stop_words = set(stopwords.words('english'))
    tokens = word_tokenize(text)
    return ' '.join([word for word in tokens if word not in stop_words])

def lemmatize_text_spacy(text):
    '''
    Lemmatize text using spaCy, considering the part-of-speech and context of each word.
    '''
    doc = nlp(text)    
    lemmatized_output = ' '.join([token.lemma_ for token in doc])
    return lemmatized_output

def clean_text(text):
    '''
    Apply all cleaning functions to text
    '''
    if not isinstance(text, str) or not text.strip():
        return ""
    text = expand_contractions(text)
    text = replace_emoji(text)
    text = lowercase_text(text)
    text = remove_special_characters(text)
    text = remove_punctuation(text)
    text = remove_numbers(text)
    text = remove_whitespace(text)
    text = remove_stopwords(text)
    lemmatized_output = lemmatize_text_spacy(text)
    return lemmatized_output