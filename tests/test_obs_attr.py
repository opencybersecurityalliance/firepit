from .helpers import tmp_storage


def test_obs_attr_url(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    data = store.extract_observeddata_attribute(
        'url',
        'last_observed')
    assert len(data) == 31
    assert 'last_observed' in data[0]


def test_obs_attr_url_only(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    data = store.extract_observeddata_attribute('url', 'id', path=['value'])
    assert len(data) == 31
    assert set(data[0].keys()) == {'observation_id', 'value'}


def test_obs_attr_multiple(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    attrs = [
        'number_observed',
        'first_observed',
        'last_observed',
        'id',
    ]
    data = store.extract_observeddata_attribute('url', attrs)
    assert len(data) == 31
    assert set(data[0].keys()) == {
        'number_observed',
        'first_observed',
        'last_observed',
        'observation_id',
        'value',
        'id',  # The original url:id
    }    
