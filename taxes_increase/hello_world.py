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


