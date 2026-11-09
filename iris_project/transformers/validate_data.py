import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def validate(df, *args, **kwargs):
    # Check for missing values
    null_count = int(df.isnull().sum().sum())
    print('Missing values found:', null_count)

    # Check number of distinct target classes
    n_classes = int(df['target'].nunique())
    print('Unique classes found:', n_classes)

    return df


@test
def test_no_nulls_and_three_classes(output, *args):
    assert output.isnull().sum().sum() == 0, 'Dataset contains missing values'
    assert output['target'].nunique() == 3, f'Expected 3 classes, found {output["target"].nunique()}'

