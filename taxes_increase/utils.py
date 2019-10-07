import logging
import pandas as pd
import numpy as np


def setup_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger

logger = setup_logger(__name__)

def bootstrap(df, samplesize, nsamples, split=['new_repeat', 'service']):
    ''' return a dataframe containing samplesize * nsamples rows from each
    "partition" (by split) of df, sampled with replacement. A column sample_id
    is added to enable grouping.

     split must be a column name or list of column names. Columns that don't
     occur in df will be ignored. If you don't need df partitioned, pass None
     or an empty list (or a list with no overlap with the actual column names).

     Each requester_id must occur at most once per partition!'''

    if type(split) != list: split = [split]
    split = [col for col in split if col in df]

    get_samples = lambda df: (
        df
            .sample(nsamples * samplesize, replace=True)
            .assign(sample_id=np.arange(nsamples).repeat(samplesize))
    )

    if split:
        partitions = (
            df[np.all(df[split] == key, axis=1)]
            for key in df[split].drop_duplicates().values
        )
        return pd.concat([get_samples(x) for x in partitions])
    else:
        return get_samples(df)


def maketable(df, splitter, means, var10k, thing):
    getsamplesize = lambda SE, table: table.var10k * 10000 * SE ** -2

    table = df[splitter + ['num_owners']]
    table = (
        table
            .join(means[[thing]], on=splitter)
            .join(var10k[[thing]], on=splitter, rsuffix='_var10k')
            .rename(columns={thing + '_var10k': 'var10k'})
    )
    table['sample_size05'] = getsamplesize(.05 * table[thing] / 1.96, table).astype(int)
    table['sample_size02'] = getsamplesize(.02 * table[thing] / 1.96, table).astype(int)
    table['30 day detectable'] = 2.49 * np.sqrt(table.var10k * 10000 * 8 / table.num_owners) / table[thing]
    table = (
        table
            .rename(
            columns={
                thing: 'average ' + thing
                , 'num_owners': '30 Day Owner Volume'
            }
        ).drop(columns='var10k')
            .groupby(splitter)
            .min()
    )
    table['30 day detectable'] = table['30 day detectable'].apply('{:.1%}'.format)
    return table


def split_data(df, split, **kwargs):
    """
    Split inputted data by the inputted list of split proportions

    Args:
        df (pandas.DataFrame): data to be split
        split (list): list of split proportions summing to 1

    Returns:
        (dictionary):
            keys: integers from 1 to `splits`
            values: pandas.DataFrame's containing partial data from `data`
    """
    if 'seed' in kwargs:
        np.random.seed(kwargs['seed'])
    if sum(split) != 1 or not isinstance(split, list):
        logger.debug('split must be a list of floats summing to one, control is at index 0')
        raise ValueError
    df_lens = [int(x * len(df)) for x in split]
    del df_lens[-1]
    df_indexes = np.cumsum(df_lens)
    df_indexes_shifted = [x + 1 if ind > 0 else x for ind, x in enumerate(df_indexes)]
    df_splits = np.split(df.sample(frac=1), df_indexes_shifted)
    df_splits_dict = {i + 1: split for i, split in zip(range(len(split)), df_splits)}
    return df_splits_dict