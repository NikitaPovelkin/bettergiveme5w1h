import pandas as pd
from extractor.document import Document
from extractor.extractor import MasterExtractor


def prepare_tweets_from(dataframe):
    #dataframe = dataframe.drop(['id', 'username'], axis=1)

    url_regex = r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)'
    hashtag_regex = r'#[a-z0-9A-Z_]+'

    dataframe['text'] = dataframe['text'].str.replace(url_regex, '', regex=True)
    dataframe['text'] = dataframe['text'].str.replace(hashtag_regex, '', regex=True)
    dataframe['text'] = dataframe['text'].str.strip(": ")
    dataframe = dataframe[dataframe['text'].str.strip().astype(bool)]
    dataframe = dataframe.drop_duplicates(subset='text', keep="first")

    return dataframe


def evaluate_(tweets_df):
    extractor = MasterExtractor()
    result_df = pd.DataFrame(columns=['who', 'what'])

    for idx, row in tweets_df.iterrows():
        doc = Document.from_text(text=row['text'], date=row['date'])
        doc = extractor.parse(doc)
        row_df = pd.DataFrame({'who': doc.get_top_answer('who'),
                            'what': doc.get_top_answer('what')
                            # 'where': doc.get_top_answer('where'),
                            # 'when': doc.get_top_answer('when'),
                            # 'why': doc.get_top_answer('why'),
                            # 'how': doc.get_top_answer('how')
                               }
                              , index=[0])

        result_df = pd.concat([result_df, row_df], ignore_index=True, axis=0)
        print(str(idx))

    return result_df


def test_function():
    extractor = MasterExtractor()
    text = 'The Oscars film academy  condemns  Will Smiths slapping of Chris Rock and says it has launched formal review of incident'
    date = '2021-12-24T01:14:33+00:00'
    doc = Document.from_text(text=text, date=date)
    doc = extractor.parse(doc)
    who = doc.get_top_answer('who')
    print(who)


if __name__ == '__main__':
    test = True

    if test:
        test_function()
    else:
        datasets = ['bbc_breaking'] #, 'bbc_breaking', 'bbc_breaking_small', 'bbc_world', 'guardian', 'independent', 'telegraph']

        run_nr = 'run3'
        for dataset in datasets:
            tweets_df = pd.read_csv('./data/source/random_samples/' + dataset + '.csv')
            cleaned_tweets = prepare_tweets_from(tweets_df)
            results_df = evaluate_(cleaned_tweets)
            results_df['text'] = cleaned_tweets['text'].to_numpy()
            results_df.to_csv('./data/' + run_nr + '/' + dataset + '_mintreelength.csv')



