from markets_core.domain.series import SeriesId


def test_series_id_creation():
    sid = SeriesId(source="FRED", native_id="DGS10")
    assert sid.source == "fred"  # lowercased
    assert str(sid) == "fred:DGS10"