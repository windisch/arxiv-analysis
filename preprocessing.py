"""
Module that prepares the data
"""

import collections
import nltk
import pickle
import pandas as pd
from nltk.stem import WordNetLemmatizer


def load(filepath=None):
    """
    Loads a file
    """
    with open(filepath, 'rb') as f:
        data = pickle.load(f)

    df = pd.DataFrame(data).transpose()
    df.published = pd.to_datetime(df.published)
    df = df.drop('id', axis=1)

    return df


def tokenize(text):
    """
    Tokenize a sentence
    """

    tokens = nltk.word_tokenize(text)

    # Remove puncutation characters
    tokens = [_remove_punctuation(t) for t in tokens]

    tokens = [_normalize(t) for t in tokens]

    # Remove tokens related to latex stuff
    tokens = _remove_latex(tokens)

    # Stem tokens (remove tense, plurals)
    tokens = _stem(tokens)

    # Remove tokens smaller than 5 letters
    tokens = _filter(tokens, length=5)

    tokens = _remove_non_nouns(tokens)

    return tokens


def _remove_non_nouns(tokens):

    tagged = nltk.pos_tag(tokens)

    return [token for token, t_type in tagged if t_type[0] == 'N']


def _stem(tokens):

    stemmer = WordNetLemmatizer()

    return [stemmer.lemmatize(t) for t in tokens]


def _is_latex_token(token):

    if len(token) == 0:
        return False

    if token[0] == "$":
        return True

    if token[:1] == "\\":
        return True

    return False


def _remove_latex(tokens):

    return [t for t in tokens if not _is_latex_token(t)]


def _filter(tokens, length=4):
    """
    """
    return [t for t in tokens if len(t) >= length]


def _remove_punctuation(token):
    """
    Removes punctuation character from a single token
    """

    token = token.replace(",", "")
    token = token.replace(".", "")
    token = token.replace(":", "")
    token = token.replace("?", "")
    token = token.replace("!", "")

    return token


def _normalize(token):
    """
    Stemming? Lowercase
    """
    return token.lower()



def token_count(tokens):
    return dict(collections.Counter(tokens))


def compute_incidence_matrix(df):
    """
    Args:
        df_token (pandas.Series): Series where the values are tokens
    """

    return df.apply(lambda tokens: pd.Series(dict([(t, 1) for t in tokens])))


def compute_frequencies(df=None, grouper=None):

    # Compute incidence, i.e. which token appears in which paper
    df_incidence = compute_incidence_matrix(df)

    df_frequencies = df_incidence.groupby(grouper).apply(lambda x: x.sum()/x.shape[0])

    return df_frequencies


