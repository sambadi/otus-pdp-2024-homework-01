from datetime import date
from pathlib import Path
from unittest.mock import patch

from homework_01.report_preparing import _calculate_hash, _get_latest_log_info, LogInfo

ROOT_PATH = Path(__file__).parent


def test_file_hash_calculation():
    assert (
        _calculate_hash(ROOT_PATH.joinpath("test_data", "test.hash"))
        == "e10adc3949ba59abbe56e057f20f883e"
    )


def test_get_latest_log_info():
    with patch(
        "homework_01.report_preparing._calculate_hash",
        return_value="e10adc3949ba59abbe56e057f20f883e",
    ):
        with patch(
            "homework_01.report_preparing.os.path.isdir",
            return_value=True,
        ):
            with patch(
                "homework_01.report_preparing.os.path.exists",
                return_value=True,
            ):
                with patch(
                    "homework_01.report_preparing.os.listdir",
                    return_value=[
                        "test.log-20240104",
                        "test.log-20240904.gz",
                        "test.log-20240804",
                    ],
                ):
                    res = _get_latest_log_info("test_path", "test.log")
                    assert res == LogInfo(
                        filename="test.log-20240904.gz",
                        full_path="test_path\\test.log-20240904.gz",
                        hash="e10adc3949ba59abbe56e057f20f883e",
                        log_date=date(2024, 9, 4),
                        is_gzipped=True,
                    )
