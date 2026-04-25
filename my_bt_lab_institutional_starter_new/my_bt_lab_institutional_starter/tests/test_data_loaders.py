from __future__ import annotations

import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class DataLoaderTests(unittest.TestCase):
    def _import_loaders_df(self):
        pandas_backup = sys.modules.get("pandas")
        normalize_backup = sys.modules.get("my_bt_lab.data.normalize")
        tushare_backup = sys.modules.get("my_bt_lab.data.tushare_loader")
        loaders_backup = sys.modules.pop("my_bt_lab.data.loaders_df", None)

        fake_pandas = types.SimpleNamespace(
            DataFrame=dict,
            to_datetime=lambda value, **kwargs: value,
        )
        fake_normalize = types.SimpleNamespace(normalize_ohlcv_df=lambda **kwargs: kwargs.get("df_raw"))
        fake_tushare = types.SimpleNamespace(fetch_tushare_ohlcv=lambda **kwargs: [])

        sys.modules["pandas"] = fake_pandas
        sys.modules["my_bt_lab.data.normalize"] = fake_normalize
        sys.modules["my_bt_lab.data.tushare_loader"] = fake_tushare

        try:
            module = importlib.import_module("my_bt_lab.data.loaders_df")
        finally:
            if loaders_backup is not None:
                sys.modules["my_bt_lab.data.loaders_df_backup"] = loaders_backup

        return module, pandas_backup, normalize_backup, tushare_backup, loaders_backup

    def _restore_modules(self, pandas_backup, normalize_backup, tushare_backup, loaders_backup):
        sys.modules.pop("my_bt_lab.data.loaders_df", None)
        sys.modules.pop("my_bt_lab.data.loaders_df_backup", None)
        if loaders_backup is not None:
            sys.modules["my_bt_lab.data.loaders_df"] = loaders_backup
        if pandas_backup is not None:
            sys.modules["pandas"] = pandas_backup
        else:
            sys.modules.pop("pandas", None)
        if normalize_backup is not None:
            sys.modules["my_bt_lab.data.normalize"] = normalize_backup
        else:
            sys.modules.pop("my_bt_lab.data.normalize", None)
        if tushare_backup is not None:
            sys.modules["my_bt_lab.data.tushare_loader"] = tushare_backup
        else:
            sys.modules.pop("my_bt_lab.data.tushare_loader", None)

    def test_load_data_item_to_df_supports_excel_source(self):
        module, pandas_backup, normalize_backup, tushare_backup, loaders_backup = self._import_loaders_df()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                project_root = Path(tmp)
                item = {"name": "excel_demo", "source": "excel", "excel": "demo.xlsx"}
                expected = [{"datetime": "2026-01-01", "close": 1.0}]
                with patch.object(module, "load_excel_item_to_df", return_value=expected) as mock_excel:
                    rows = module.load_data_item_to_df(item=item, project_root=project_root, cfg={})
                self.assertEqual(rows, expected)
                mock_excel.assert_called_once()
        finally:
            self._restore_modules(pandas_backup, normalize_backup, tushare_backup, loaders_backup)

    def test_load_data_item_to_df_supports_db_alias(self):
        module, pandas_backup, normalize_backup, tushare_backup, loaders_backup = self._import_loaders_df()
        try:
            item = {"name": "db_demo", "source": "db", "code": "000001.SZ"}
            expected = [{"datetime": "2026-01-01", "close": 10.5}]
            with patch.object(module, "load_postgres_item_to_df", return_value=expected) as mock_pg:
                rows = module.load_data_item_to_df(item=item, project_root=Path("."), cfg={"postgres": {}})
            self.assertEqual(rows, expected)
            mock_pg.assert_called_once()
        finally:
            self._restore_modules(pandas_backup, normalize_backup, tushare_backup, loaders_backup)


if __name__ == "__main__":
    unittest.main()
