"""Uploading your own files: aalibrary.egress, RawFile's datagram data and
LocalSurvey (GitHub issues #19-#25).

Nothing here reaches Google, NCEI or ICES: the bucket is a fake held in
memory, BigQuery and S3 are faked, and the ICES ship list is a short fake.
The .raw files are small synthetic EK60 files (aalibrary.utils.ek60_synth).
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pytest

from aalibrary import egress, ices_ship_names, metadata, raw_file, survey
from aalibrary.raw_file import RawFile
from aalibrary.utils import cloud_utils, get_datagram_data
from aalibrary.utils.ek60_synth import write_ek60_raw

BUCKET = "ggn-nmfs-aa-prod-1-data"
ICES = [
    {"key": "33HH", "description": "HENRY B. BIGELOW"},
    {"key": "33RL", "description": "REUBEN LASKER"},
]


# --------------------------------------------------------------- the fakes
class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket, self.name = bucket, name

    def exists(self):
        return self.name in self.bucket.objects

    def upload_from_filename(self, filename):
        with open(filename, "rb") as f:  # a folder raises, as GCS's does
            self.bucket.objects[self.name] = f.read()

    def download_as_bytes(self, start=None, end=None):
        data = self.bucket.objects[self.name]
        self.bucket.ranges.append((start, end))
        return data[start : None if end is None else end + 1]


class FakeBucket:
    def __init__(self, name=BUCKET):
        self.name = name
        self.client = object()
        self.objects: dict[str, bytes] = {}
        self.ranges: list = []

    def blob(self, name, chunk_size=None):
        return FakeBlob(self, name)


class FakeBigQuery:
    """What the upload functions ask BigQuery, and what they write."""

    def __init__(self):
        self.can_write = True
        self.recorded: dict[str, set] = {}  # table -> GCP_URIs with a row
        self.rows: list[tuple[str, object]] = []
        self.refuse_writes = False

    def check(self, tables=(), project_id=None):
        return None if self.can_write else "Your Google account can't add rows."

    def already(self, uris, table, project_id=None):
        return set(uris) & self.recorded.get(table, set())

    def write(self, table, location):
        def _write(rf=None, user_email="", debug=False):
            if self.refuse_writes:
                raise RuntimeError(
                    "Reason: 403 POST https://bigquery.googleapis.com/upload: "
                    "Access Denied: Permission bigquery.tables.create denied"
                )
            self.rows.append((table, rf))
            self.recorded.setdefault(table, set()).add(getattr(rf, location))

        return _write


@pytest.fixture
def bucket(monkeypatch):
    fake = FakeBucket()
    monkeypatch.setattr(
        cloud_utils, "setup_gcp_storage_objs", lambda *a, **k: (None, fake.name, fake)
    )
    return fake


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    """ICES, S3 and NCEI answer from fakes."""

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return ICES

    monkeypatch.setattr(ices_ship_names.requests, "get", lambda **k: Response())
    ices_ship_names._fetch_all_ship_info.cache_clear()
    ices_ship_names._normalized_ices_ship_names.cache_clear()
    monkeypatch.setattr(
        cloud_utils, "create_s3_objs", lambda *a, **k: (None, None, None)
    )
    monkeypatch.setattr(cloud_utils, "check_if_file_exists_in_s3", lambda **k: False)
    monkeypatch.setattr(
        raw_file, "check_if_tugboat_metadata_json_exists_in_survey", lambda **k: None
    )
    yield
    ices_ship_names._fetch_all_ship_info.cache_clear()
    ices_ship_names._normalized_ices_ship_names.cache_clear()


@pytest.fixture
def bq(monkeypatch):
    fake = FakeBigQuery()
    monkeypatch.setattr(metadata, "check_metadata_write_access", fake.check)
    monkeypatch.setattr(metadata, "metadata_already_recorded", fake.already)
    monkeypatch.setattr(
        metadata,
        "create_and_upload_metadata_df_for_raw",
        fake.write(metadata.RAW_FILE_METADATA_TABLE, "raw_gcp_storage_bucket_location"),
    )
    monkeypatch.setattr(
        metadata,
        "create_and_upload_metadata_df_for_netcdf",
        fake.write(
            metadata.NETCDF_FILE_METADATA_TABLE, "netcdf_gcp_storage_bucket_location"
        ),
    )
    return fake


@pytest.fixture
def testdata(tmp_path):
    """Mike's folder: three .raw/.idx pairs and a .nc file."""
    folder = tmp_path / "testdata"
    folder.mkdir()
    for stamp in ("T211129", "T211342", "T211529"):
        write_ek60_raw(folder / f"D20231017-{stamp}.raw", n_pings=2)
        (folder / f"D20231017-{stamp}.idx").write_bytes(b"idx " + stamp.encode())
    (folder / "D20231017-T211129.nc").write_bytes(b"netcdf")
    return folder


STANDARD = "HDD/Henry_B_Bigelow/DX2301/EK80/data/raw/D20231017-T211129.idx"
TEST_FOLDER = (
    f"{BUCKET}\\HDD\\Henry B. Bigelow\\DX2301\\EK80\\test"  # str(Path) on Windows
)


def _upload(f: Path, bucket, **overrides):
    """issue #25's call, with Mike's parameters."""
    params = {
        "file_name": str(f.name),
        "file_type": str(f.suffix[1:]),
        "ship_name": "Henry B. Bigelow",
        "survey_name": "DX2301",
        "echosounder": "EK80",
        "file_location": str(f.parent),
        "gcp_storage_bucket_location": TEST_FOLDER,
        "gcp_bucket": bucket,
        "data_source": "HDD",
        "user_email": "michael.jech@noaa.gov",
        "is_survey_metadata": False,
        "is_calibration_file": False,
        "is_calibration_mapping_file": False,
        "is_auxiliary_file": False,
        "is_derived_product": False,
        "verbose": True,
        "debug": True,
    }
    params.update(overrides)
    params = {k: v for k, v in params.items() if v is not _OMIT}
    return egress.upload_file_to_gcp_storage_bucket(**params)


_OMIT = object()
WANTED = "HDD/Henry B. Bigelow/DX2301/EK80/test/D20231017-T211129.idx"


# ------------------------------------------- issue #25: upload one file
@pytest.mark.parametrize(
    "result, overrides",
    [
        ("1: file_name + its folder", {}),
        ("2: no file_name", {"file_name": _OMIT, "file_location": "FILE"}),
        ("2: file_name is the full path", {"file_name": "FILE"}),
        ("3: both the full path", {"file_name": "FILE", "file_location": "FILE"}),
        ("5: no file_name, full path", {"file_name": _OMIT, "file_location": "FILE"}),
        (
            "6: the bucket location names the file",
            {
                "file_name": _OMIT,
                "file_location": "FILE",
                "gcp_storage_bucket_location": TEST_FOLDER + "\\D20231017-T211129.idx",
            },
        ),
        (
            "a gs:// URI",
            {
                "gcp_storage_bucket_location": f"gs://{BUCKET}/HDD/Henry B. Bigelow/DX2301/EK80/test/"
            },
        ),
    ],
)
def test_each_of_mikes_calls_puts_the_file_in_his_test_folder(
    testdata, bucket, result, overrides
):
    f = testdata / "D20231017-T211129.idx"
    overrides = {k: (str(f) if v == "FILE" else v) for k, v in overrides.items()}

    assert _upload(f, bucket, **overrides) == WANTED
    assert list(bucket.objects) == [WANTED]  # not one object named after the folder
    assert bucket.objects[WANTED] == f.read_bytes()


def test_without_a_bucket_location_it_goes_where_the_directory_uploader_puts_it(
    testdata, bucket
):
    f = testdata / "D20231017-T211129.idx"
    # Result 7: the standard location, with the ship name as the directory
    # uploader writes it (it used to be "Henry B. Bigelow" here).
    assert _upload(f, bucket, gcp_storage_bucket_location=_OMIT) == STANDARD
    assert list(bucket.objects) == [STANDARD]


def test_result_4_still_needs_to_know_where_the_file_is(testdata, bucket):
    f = testdata / "D20231017-T211129.idx"
    with pytest.raises(AssertionError, match="file_location"):
        _upload(f, bucket, file_location=_OMIT)  # only a bare name: no path
    with pytest.raises(AssertionError, match="is a folder"):
        _upload(f, bucket, file_name=_OMIT)  # a folder, but which file?
    assert bucket.objects == {}


def test_a_file_already_in_the_bucket_is_not_uploaded_again(testdata, bucket):
    f = testdata / "D20231017-T211129.idx"
    bucket.objects[WANTED] = b"already there"
    assert _upload(f, bucket) == WANTED
    assert bucket.objects[WANTED] == b"already there"


def test_a_missing_file_is_reported_and_nothing_is_uploaded(testdata, bucket, caplog):
    with caplog.at_level(logging.ERROR):
        assert _upload(testdata / "nope.idx", bucket) is None
    assert bucket.objects == {}
    assert "isn't a file on this computer" in caplog.text


@pytest.mark.parametrize(
    "location, expected",
    [
        ("HDD/Henry_B_Bigelow/test/x.raw", "HDD/Henry_B_Bigelow/test/x.raw"),
        (
            "HDD/Henry_B_Bigelow/test/renamed.raw",
            "HDD/Henry_B_Bigelow/test/renamed.raw",
        ),
        ("HDD/Henry_B_Bigelow/test/", "HDD/Henry_B_Bigelow/test/x.raw"),
        ("HDD\\Henry_B_Bigelow\\test", "HDD/Henry_B_Bigelow/test/x.raw"),
        # folder names with dots aren't mistaken for file names
        ("HDD/Henry_B._Bigelow", "HDD/Henry_B._Bigelow/x.raw"),
        ("HDD/Henry B. Bigelow", "HDD/Henry B. Bigelow/x.raw"),
        (f"/{BUCKET}/HDD//x.raw", "HDD/x.raw"),
        (BUCKET, "x.raw"),
    ],
)
def test_bucket_locations(location, expected):
    assert egress._bucket_object_path(location, "x.raw", FakeBucket()) == expected


def test_a_gs_uri_for_another_bucket_is_refused():
    with pytest.raises(ValueError, match="other-bucket"):
        egress._bucket_object_path("gs://other-bucket/HDD/", "x.raw", FakeBucket())


# ---------------------------------- issue #22: the directory uploader
def _upload_directory(folder, bucket, **overrides):
    params = {
        "local_echosounder_directory_to_upload": folder,
        "ship_name": "Henry B. Bigelow",
        "survey_name": "DX2301",
        "echosounder": "EK80",
        "data_source": "HDD",
        "gcp_bucket": bucket,
    }
    params.update(overrides)
    return egress.upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
        **params
    )


RAW_DIR = "HDD/Henry_B_Bigelow/DX2301/EK80/data/raw/"
NC_DIR = "HDD/Henry_B_Bigelow/DX2301/EK80/data/netcdf/"


def test_without_bigquery_access_nothing_is_uploaded(testdata, bucket, bq):
    bq.can_write = False
    with pytest.raises(PermissionError, match="Nothing has been uploaded") as raised:
        _upload_directory(testdata, bucket)
    assert "upload_metadata=False" in str(raised.value)
    assert bucket.objects == {}
    assert bq.rows == []


def test_upload_metadata_false_uploads_the_files_and_leaves_bigquery_alone(
    testdata, bucket, bq
):
    bq.can_write = False  # not even asked
    _upload_directory(testdata, bucket, upload_metadata=False)
    assert sorted(bucket.objects) == sorted(
        [
            RAW_DIR + f"D20231017-{t}.{x}"
            for t in ("T211129", "T211342", "T211529")
            for x in ("raw", "idx")
        ]
        + [NC_DIR + "D20231017-T211129.nc"]
    )
    assert bq.rows == []


def test_files_then_their_metadata_once_including_the_netcdf(testdata, bucket, bq):
    _upload_directory(testdata, bucket)
    assert len(bucket.objects) == 7
    tables = sorted(table for table, _ in bq.rows)
    assert tables == [metadata.RAW_FILE_METADATA_TABLE] * 3 + [
        metadata.NETCDF_FILE_METADATA_TABLE
    ]
    # issue #21: the .raw rows carry the time zone and transect from the files
    for table, rf in bq.rows:
        if table == metadata.RAW_FILE_METADATA_TABLE:
            assert rf.file_datetime_timezone == "UTC"
            assert rf.transect_name == "T1"
            assert rf.raw_file_exists_in_gcp and rf.idx_file_exists_in_gcp
            assert rf.gcp_bucket is bucket

    # Run again: nothing uploaded, no second rows.
    before = dict(bucket.objects)
    _upload_directory(testdata, bucket)
    assert bucket.objects == before
    assert len(bq.rows) == 4


def test_metadata_later_records_only_what_is_missing(testdata, bucket, bq):
    _upload_directory(testdata, bucket, upload_metadata=False)
    assert bq.rows == []
    egress.upload_local_echosounder_metadata_from_directory_to_bigquery(
        local_echosounder_directory=testdata,
        ship_name="Henry B. Bigelow",
        survey_name="DX2301",
        echosounder="EK80",
        data_source="HDD",
        gcp_bucket=bucket,
    )
    assert len(bq.rows) == 4
    egress.upload_local_echosounder_metadata_from_directory_to_bigquery(
        local_echosounder_directory=testdata,
        ship_name="Henry B. Bigelow",
        survey_name="DX2301",
        echosounder="EK80",
        data_source="HDD",
        gcp_bucket=bucket,
    )
    assert len(bq.rows) == 4


def test_a_refused_write_stops_with_the_way_to_finish(testdata, bucket, bq):
    bq.refuse_writes = True  # the check said yes, the write said no
    with pytest.raises(PermissionError, match="upload_local_echosounder_metadata"):
        _upload_directory(testdata, bucket)
    assert len(bucket.objects) == 7  # the files are up; rerun records the rest


def test_metadata_is_not_recorded_for_a_file_that_failed_to_upload(
    testdata, bucket, bq, monkeypatch
):
    real = cloud_utils.upload_file_to_gcp_bucket

    def flaky(bucket, blob_file_path, local_file_path, debug=False):
        if blob_file_path.endswith("T211342.raw"):
            raise OSError("network")
        return real(
            bucket=bucket,
            blob_file_path=blob_file_path,
            local_file_path=local_file_path,
        )

    monkeypatch.setattr(cloud_utils, "upload_file_to_gcp_bucket", flaky)
    _upload_directory(testdata, bucket)
    names = sorted(
        rf.raw_file_name for _, rf in bq.rows if rf.raw_file_name.endswith(".raw")
    )
    assert names == ["D20231017-T211129.raw", "D20231017-T211529.raw"]


# -------------------------------------------------- issue #20: ship names
def test_an_unlisted_ship_is_refused_before_anything_is_uploaded(testdata, bucket, bq):
    with pytest.raises(ValueError, match="provisional_ship_name=True") as raised:
        _upload_directory(testdata, bucket, ship_name="Henry B Bigelw")
    assert "Henry_B_Bigelow" in str(raised.value)  # the suggestion
    assert bucket.objects == {}


def test_a_provisional_ship_name_is_uploaded_with_no_ices_code(testdata, bucket, bq):
    _upload_directory(testdata, bucket, ship_name="DriX 1", provisional_ship_name=True)
    assert any(
        path.startswith("HDD/Drix_1/DX2301/EK80/data/raw/") for path in bucket.objects
    )
    raws = [rf for table, rf in bq.rows if table == metadata.RAW_FILE_METADATA_TABLE]
    assert len(raws) == 3 and all(rf.ices_code == "" for rf in raws)


def test_without_metadata_an_unlisted_ship_is_only_a_warning(
    testdata, bucket, bq, caplog
):
    with caplog.at_level(logging.WARNING):
        _upload_directory(testdata, bucket, ship_name="DriX 1", upload_metadata=False)
    assert "isn't in the ICES ship list" in caplog.text
    assert len(bucket.objects) == 7


def test_a_misspelt_data_source_is_refused_before_uploading(testdata, bucket, bq):
    with pytest.raises(AssertionError, match="data source"):
        _upload_directory(testdata, bucket, data_source="hdd")
    assert bucket.objects == {}


# ----------------------------------------- issue #21: RawFile's datagram
def _raw_file(folder, data_source="HDD", **overrides):
    params = {
        "file_name": "D20231017-T211129.raw",
        "file_type": "raw",
        "ship_name": "Henry B. Bigelow",
        "survey_name": "DX2301",
        "echosounder": "EK80",
        "data_source": data_source,
        "file_download_directory": str(folder),
        "gcp_bucket": FakeBucket(),
        "gcp_bucket_name": BUCKET,
        "gcp_stor_client": object(),
    }
    params.update(overrides)
    return RawFile(**params)


def test_a_local_raw_file_gives_its_time_zone_and_transect(testdata):
    rf = _raw_file(testdata)
    assert rf.file_datetime_timezone == "UTC"
    assert rf.transect_name == "T1"
    assert rf.datagram_dict["type"] == "CON0"


def test_without_the_file_they_are_none_for_any_data_source(tmp_path):
    for source in ("HDD", "OMAO", "TEST"):
        rf = _raw_file(tmp_path, data_source=source)
        assert (rf.datagram_timestamp, rf.file_datetime_timezone, rf.transect_name) == (
            None,
            None,
            None,
        )


def test_ncei_files_get_them_too(testdata, monkeypatch):
    # stream_datagram_dict_from_ncei returns a list of datagrams; the old code
    # looked for "timestamp" in the list itself, so NCEI rows never had them.
    monkeypatch.setattr(cloud_utils, "check_if_file_exists_in_s3", lambda **k: True)
    monkeypatch.setattr(
        raw_file.utils.ncei_utils,
        "get_closest_ncei_formatted_ship_name",
        lambda **k: "Henry_B._Bigelow",
    )
    datagrams = get_datagram_data.get_datagram_dict_from_raw_file(
        str(testdata / "D20231017-T211129.raw"), {}
    )
    monkeypatch.setattr(
        raw_file,
        "stream_datagram_dict_from_ncei",
        lambda **k: [datagrams, {"type": "NME0"}],
    )
    rf = _raw_file(testdata / "elsewhere", data_source="NCEI")
    assert (rf.file_datetime_timezone, rf.transect_name) == ("UTC", "T1")


def test_a_raw_file_without_a_download_directory_can_be_made(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rf = RawFile(
        file_name="D20231017-T211129.nc",
        file_type="netcdf",
        ship_name="Henry_B_Bigelow",
        survey_name="DX2301",
        echosounder="EK80",
        data_source="HDD",
        gcp_bucket=FakeBucket(),
        gcp_bucket_name=BUCKET,
        gcp_stor_client=object(),
    )  # raised KeyError: 'file_download_directory'
    assert rf.netcdf_gcp_storage_bucket_location == NC_DIR + "D20231017-T211129.nc"


def test_the_metadata_row_has_them(testdata, monkeypatch):
    sent = []
    monkeypatch.setattr(metadata.pandas_gbq, "to_gbq", lambda **k: sent.append(k))
    metadata.create_and_upload_metadata_df_for_raw(
        rf=_raw_file(testdata), user_email="michael.jech@noaa.gov"
    )
    row = sent[0]["dataframe"].iloc[0]
    assert (row["FILE_DATETIME_TIMEZONE"], row["TRANSECT"], row["UPLOADED_BY"]) == (
        "UTC",
        "T1",
        "michael.jech",
    )
    assert sent[0]["destination_table"] == "metadata.aalibrary_file_metadata"


# --------------------- the datagram is streamed, not the whole file
def test_streaming_reads_only_the_first_datagram(testdata, monkeypatch):
    data = (testdata / "D20231017-T211129.raw").read_bytes()
    first = 4 + int.from_bytes(data[:4], "little") + 4
    assert first < len(data) / 4

    bucket = FakeBucket()
    bucket.objects["x.raw"] = data
    datagrams = get_datagram_data.stream_datagram_dict_from_gcs("x.raw", BUCKET, bucket)
    assert datagrams[0]["type"] == "CON0" and datagrams[0]["transect_name"] == "T1"
    assert bucket.ranges == [(0, 3), (0, first - 1)]  # not the whole file

    class S3:
        def __init__(self):
            self.ranges = []

        def get_object(self, Bucket, Key, Range):
            start, end = (int(x) for x in Range.split("=")[1].split("-"))
            self.ranges.append((start, end))
            return {"Body": io.BytesIO(data[start : end + 1])}

    s3 = S3()
    monkeypatch.setattr(
        get_datagram_data, "create_s3_objs", lambda *a, **k: (s3, None, None)
    )
    datagrams = get_datagram_data.stream_datagram_dict_from_ncei("data/raw/x.raw")
    assert datagrams[0]["timestamp"].year == 2016
    assert s3.ranges == [(0, 3), (0, first - 1)]


def test_streaming_something_that_isnt_a_raw_file():
    bucket = FakeBucket()
    bucket.objects["x.raw"] = b"<html>not found</html>"
    assert (
        get_datagram_data.stream_datagram_dict_from_gcs("x.raw", BUCKET, bucket) == {}
    )


# ---------------------------------- importing doesn't need ICES to answer
def test_importing_doesnt_ask_ices():
    # In a fresh interpreter where any web request fails: importing used to
    # raise (the ICES ship list was fetched when raw_file.py was imported).
    import subprocess
    import sys

    code = (
        "import requests\n"
        "def refuse(*a, **k):\n"
        "    raise OSError('no network')\n"
        "requests.get = refuse\n"
        "import aalibrary.egress, aalibrary.survey, aalibrary.metadata\n"
        "print('imported')\n"
    )
    src = str(Path(egress.__file__).resolve().parents[1])
    done = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": src},
        timeout=300,
        check=False,
    )
    assert done.stdout.strip().endswith("imported"), done.stderr[-2000:]


def test_ices_is_asked_once(monkeypatch):
    calls = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            calls.append(1)
            return ICES

    monkeypatch.setattr(ices_ship_names.requests, "get", lambda **k: Response())
    ices_ship_names._fetch_all_ship_info.cache_clear()
    for _ in range(3):
        assert (
            ices_ship_names.get_ices_code_from_ship_name(
                "Henry_B_Bigelow", is_normalized=True
            )
            == "33HH"
        )
    assert len(calls) == 1


# ------------------------------------- BigQuery's answers, interpreted
class FakeClient:
    def __init__(self, permissions=None, error=None, rows=()):
        self.permissions, self.error, self.rows = permissions, error, rows
        self.asked = []

    def test_iam_permissions(self, table, permissions):
        self.asked.append((table, permissions))
        if self.error:
            raise self.error
        return {"permissions": self.permissions} if self.permissions else {}

    def query(self, sql, job_config=None):
        if self.error:
            raise self.error
        self.sql, self.params = sql, job_config.query_parameters

        class Job:
            def result(job):
                return [{"GCP_URI": uri} for uri in self.rows]

        return Job()


def test_the_write_check_names_the_table_and_permission(monkeypatch):
    client = FakeClient(permissions=None)
    monkeypatch.setattr(metadata.bigquery, "Client", lambda project=None: client)
    problem = metadata.check_metadata_write_access(project_id="ggn-nmfs-aa-prod-1")
    assert "ggn-nmfs-aa-prod-1:metadata.aalibrary_file_metadata" in problem
    assert "bigquery.tables.updateData" in problem
    assert client.asked == [
        (
            "ggn-nmfs-aa-prod-1.metadata.aalibrary_file_metadata",
            ["bigquery.tables.updateData"],
        )
    ]

    client.permissions = ["bigquery.tables.updateData"]
    assert metadata.check_metadata_write_access(project_id="p") is None


def test_when_bigquery_cant_be_asked_the_upload_isnt_blocked(monkeypatch):
    monkeypatch.setattr(
        metadata.bigquery,
        "Client",
        lambda project=None: FakeClient(error=OSError("offline")),
    )
    assert metadata.check_metadata_write_access(project_id="p") is None


def test_rows_already_recorded(monkeypatch):
    client = FakeClient(rows=["a/x.raw"])
    monkeypatch.setattr(metadata.bigquery, "Client", lambda project=None: client)
    assert metadata.metadata_already_recorded(
        ["a/x.raw", "a/y.raw", "a/x.raw"], "metadata.t", "p"
    ) == {"a/x.raw"}
    assert "IN UNNEST(@uris)" in client.sql and client.params[0].values == [
        "a/x.raw",
        "a/y.raw",
    ]

    monkeypatch.setattr(
        metadata.bigquery, "Client", lambda project=None: FakeClient(error=OSError())
    )
    assert metadata.metadata_already_recorded(["a"], "metadata.t", "p") is None


def test_permission_errors_are_recognised():
    from google.api_core import exceptions

    try:
        try:
            raise exceptions.Forbidden("Access Denied")
        except Exception as e:
            raise RuntimeError("Reason: wrapped") from e
    except RuntimeError as wrapped:
        assert metadata.is_permission_error(wrapped)
    assert metadata.is_permission_error(
        RuntimeError("Reason: 403 POST ...: Permission bigquery.tables.create denied")
    )
    assert not metadata.is_permission_error(
        ValueError("time data '' does not match format")
    )


# ---------------------------------------------- issue #19: LocalSurvey
def _survey(folder, bucket, **overrides):
    params = {
        "directory_path": str(folder),
        "ship_name": "Henry B. Bigelow",
        "survey_name": "DX2301",
        "echosounder": "EK80",
        "data_source": "HDD",
        "gcp_bucket": bucket,
    }
    params.update(overrides)
    return survey.LocalSurvey(**params)


def test_local_survey_uploads_to_the_standard_locations(testdata, bucket):
    s = _survey(testdata, bucket)
    s.upload_files()
    assert sorted(bucket.objects) == sorted(
        [
            RAW_DIR + f"D20231017-{t}.{x}"
            for t in ("T211129", "T211342", "T211529")
            for x in ("raw", "idx")
        ]
        + [NC_DIR + "D20231017-T211129.nc"]
    )


def test_local_survey_without_an_echosounder_uploads_nothing(testdata, bucket):
    s = _survey(testdata, bucket, echosounder="")  # "testdata" isn't an echosounder
    with pytest.raises(ValueError, match='echosounder="EK80"'):
        s.upload_files()
    assert bucket.objects == {}


def test_local_survey_finds_the_echosounder_from_the_folder(testdata, bucket):
    ek80 = testdata.parent / "DX2301" / "EK80"
    ek80.parent.mkdir()
    os.rename(testdata, ek80)
    _survey(ek80.parent, bucket, echosounder="").upload_files()
    assert len(bucket.objects) == 7 and all("/EK80/data/" in p for p in bucket.objects)


# -------------------------------------------- aa-upload passes them on
def test_aa_upload_passes_the_new_options_on(testdata, monkeypatch):
    from aalibrary.console import aa_upload

    seen = {}
    monkeypatch.setattr(
        egress,
        "upload_local_echosounder_files_from_directory_to_gcp_storage_bucket",
        lambda **k: seen.update(k),
    )
    args = aa_upload._build_parser().parse_args(
        [
            str(testdata),
            "--ship_name",
            "DriX 1",
            "--survey_name",
            "DX2301",
            "--sonar_model",
            "EK80",
            "--no-metadata",
            "--provisional-ship-name",
        ]
    )
    aa_upload._upload_echosounder(
        input_path=testdata,
        ship_name=args.ship_name,
        survey_name=args.survey_name,
        sonar_model=args.sonar_model,
        data_source=args.data_source,
        gcp_bucket=None,
        debug=False,
        dry_run=False,
        upload_metadata=not args.no_metadata,
        provisional_ship_name=args.provisional_ship_name,
    )
    assert (seen["upload_metadata"], seen["provisional_ship_name"]) == (False, True)

    defaults = aa_upload._build_parser().parse_args([str(testdata)])
    assert (defaults.no_metadata, defaults.provisional_ship_name) == (False, False)
